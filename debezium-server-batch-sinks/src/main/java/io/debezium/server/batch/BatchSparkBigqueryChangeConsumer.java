/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.engine.ChangeEvent;
import io.debezium.server.batch.streammapper.BigqueryStorageNameMapper;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import com.google.cloud.spark.bigquery.repackaged.com.google.auth.oauth2.GoogleCredentials;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import static org.apache.spark.sql.functions.*;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("sparkbigquerybatch")
@Dependent
public class BatchSparkBigqueryChangeConsumer extends AbstractBatchSparkChangeConsumer {

  static final HashMap<String, String> saveOptions = new HashMap<>();
  final String saveFormat = "bigquery";
  @ConfigProperty(name = "debezium.sink.batch.destination-regexp", defaultValue = "")
  protected Optional<String> destinationRegexp;
  @ConfigProperty(name = "debezium.sink.batch.destination-regexp-replace", defaultValue = "")
  protected Optional<String> destinationRegexpReplace;
  @Inject
  protected BigqueryStorageNameMapper streamMapper;
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.dataset", defaultValue = "")
  Optional<String> bqDataset;
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.project", defaultValue = "")
  Optional<String> gcpProject;
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.temporaryGcsBucket", defaultValue = "")
  Optional<String> temporaryGcsBucket;
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.createDisposition", defaultValue = "CREATE_IF_NEEDED")
  String createDisposition;
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.partitionField", defaultValue = "__source_ts")
  String partitionField;
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.partitionType", defaultValue = "MONTH")
  String partitionType;
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.allowFieldAddition", defaultValue = "true")
  String allowFieldAddition;
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.intermediateFormat", defaultValue = "parquet")
  String intermediateFormat;
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.allowFieldRelaxation", defaultValue = "true")
  String allowFieldRelaxation;
  GoogleCredentials googleCredentials;

  public void initizalize() throws InterruptedException {

    try {
      googleCredentials = GoogleCredentials.getApplicationDefault();
    } catch (IOException e) {
      e.printStackTrace();
      throw new InterruptedException("Failed to initialize Google Credentials");
    }

    if (gcpProject.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.sparkbatch.spark.datasource.bigquery.project`");
    }
    if (temporaryGcsBucket.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.sparkbatch.spark.datasource.bigquery.temporaryGcsBucket`");
    }

    streamMapper.initialize();
    super.initizalize();
  }

  @PostConstruct
  void connect() throws InterruptedException {
    this.initizalize();
    saveOptions.put("project", gcpProject.get());
    saveOptions.put("temporaryGcsBucket", temporaryGcsBucket.get());
    saveOptions.put("createDisposition", createDisposition);
    saveOptions.put("partitionField", partitionField);
    saveOptions.put("partitionType", partitionType);
    saveOptions.put("allowFieldAddition", allowFieldAddition);
    saveOptions.put("allowFieldRelaxation", allowFieldRelaxation);
    saveOptions.put("intermediateFormat", intermediateFormat);
  }

  @PreDestroy
  void close() {
    this.stopSparkSession();
  }

  protected String getClusteringFields(ChangeEvent<Object, Object> event) {

    if (event.key() == null) {
      return "__source_ts";
    }

    StructType keyFieldsSchema = BatchUtil.getSparkDfSchema(BatchUtil.getJsonSchemaNode(getString(event.key())));
    if (keyFieldsSchema == null) {
      return "__source_ts";
    }

    return StringUtils.strip(String.join(",", keyFieldsSchema.fieldNames()) + ",__source_ts", ",");
  }

  @Override
  public long uploadDestination(String destination, List<ChangeEvent<Object, Object>> data) throws InterruptedException {

    Instant start = Instant.now();

    StructType dfSchema = getSparkSchema(data.get(0));
    File jsonlines = getJsonLinesFile(destination, data);
    Dataset<Row> df;

    if (dfSchema != null) {
      LOGGER.debug("Reading data with schema definition, Schema:\n{}", dfSchema);
      df = spark.read().schema(dfSchema).json(jsonlines.getAbsolutePath());
    } else {
      LOGGER.debug("Reading data without schema definition");
      df = spark.read().json(jsonlines.getAbsolutePath());
    }

    df = df.withColumn("__source_ts", from_utc_timestamp(from_unixtime(col("__source_ts_ms").divide(1000)), "UTC"));

    if (castDeletedField) {
      df = df.withColumn("__deleted", col("__deleted").cast(DataTypes.BooleanType));
    }

    long numRecords;

    final String clusteringFields = getClusteringFields(data.get(0));
    final String tableName = streamMapper.map(destination);
    // serialize same destination uploads
    synchronized (uploadLock.computeIfAbsent(destination, k -> new Object())) {

      try {
        googleCredentials.refreshIfExpired();
      } catch (IOException e) {
        e.printStackTrace();
        throw new InterruptedException("Failed to refresh access token");
      }

      df.write()
          .mode(saveMode)
          .format(saveFormat)
          .options(saveOptions)
          .option("gcpAccessToken", googleCredentials.getAccessToken().getTokenValue())
          .option("clusteredFields", clusteringFields)
          .save(tableName);

      numRecords = df.count();
      LOGGER.debug("Uploaded {} rows to:{}, upload time:{}, saveOptions:{}, clusteredFields:{}",
          numRecords,
          tableName,
          Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS),
          saveOptions,
          clusteringFields
      );
    }

    if (LOGGER.isTraceEnabled()) {
      df.toJavaRDD().foreach(x ->
          LOGGER.trace("uploadDestination df row val:{}", x.toString())
      );
    }

    df.unpersist();
    if (jsonlines.exists()) {
      jsonlines.delete();
    }

    return numRecords;
  }

}