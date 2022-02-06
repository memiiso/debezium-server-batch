/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.spark;

import io.debezium.server.batch.BatchEvent;

import java.io.File;
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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import static org.apache.spark.sql.functions.*;

/**
 * Implementation of the consumer that delivers the messages into Bigquery destination using Spark.
 *
 * @author Ismail Simsek
 */
@Named("sparkbigquerybatch")
@Dependent
public class BatchSparkBigqueryChangeConsumer extends AbstractSparkChangeConsumer {

  static final HashMap<String, String> saveOptions = new HashMap<>();
  final String saveFormat = "bigquery";
  @ConfigProperty(name = "debezium.sink.batch.destination-regexp", defaultValue = "")
  protected Optional<String> destinationRegexp;
  @ConfigProperty(name = "debezium.sink.batch.destination-regexp-replace", defaultValue = "")
  protected Optional<String> destinationRegexpReplace;
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
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.credentialsFile", defaultValue = "")
  Optional<String> credentialsFile;

  public void initizalize() throws InterruptedException {

    if (gcpProject.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.sparkbatch.spark.datasource.bigquery.project`");
    }
    if (temporaryGcsBucket.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.sparkbatch.spark.datasource.bigquery.temporaryGcsBucket`");
    }
    if (credentialsFile.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.sparkbatch.spark.datasource.bigquery.credentialsFile`");
    }
    if (bqDataset.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.sparkbatch.spark.datasource.bigquery.dataset`");
    }
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

  public String map(String destination) {
    return bqDataset.get() + "." +
        destination
            .replaceAll(destinationRegexp.orElse(""), destinationRegexpReplace.orElse(""))
            .replace(".", "_");
  }

  @Override
  public long uploadDestination(String destination, List<BatchEvent> data) throws InterruptedException {

    Instant start = Instant.now();

    StructType dfSchema = data.get(0).getSparkDfSchema();
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

    final String clusteringFields = data.get(0).getBigQueryClusteringFields();
    final String tableName = map(destination);
    // serialize same destination uploads
    synchronized (uploadLock.computeIfAbsent(destination, k -> new Object())) {
      df.write()
          .mode(saveMode)
          .format(saveFormat)
          .options(saveOptions)
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
    jsonlines.delete();
    return numRecords;
  }

}