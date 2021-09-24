/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.engine.ChangeEvent;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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

  static HashMap<String, String> saveOptions = new HashMap<>();
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.dataset")
  String bqDataset;
  // default spark bigquery  settings
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.project")
  String gcpProject;
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.createDisposition", defaultValue = "CREATE_IF_NEEDED")
  String createDisposition;
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.partitionField", defaultValue = "__source_ts")
  String partitionField;
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.partitionType", defaultValue = "MONTH")
  String partitionType;
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.temporaryGcsBucket")
  String temporaryGcsBucket;
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.allowFieldAddition", defaultValue = "true")
  String allowFieldAddition;
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.intermediateFormat", defaultValue = "parquet")
  String intermediateFormat;
  @ConfigProperty(name = "debezium.sink.sparkbatch.save-format", defaultValue = "bigquery")
  String saveFormat;
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.allowFieldRelaxation", defaultValue = "true")
  String allowFieldRelaxation;

  @PostConstruct
  void connect() throws InterruptedException {
    this.initizalize();
    saveOptions.put("project", gcpProject);
    saveOptions.put("temporaryGcsBucket", temporaryGcsBucket);
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
  public long uploadDestination(String destination, List<ChangeEvent<Object, Object>> data) {

    Instant start = Instant.now();
    Dataset<Row> df = dataToSparkDf(destination, data);
    df = df.withColumn("__source_ts", from_utc_timestamp(from_unixtime(col("__source_ts_ms").divide(1000)), "UTC"));
    long numRecords;

    final String clusteringFields = getClusteringFields(data.get(0));
    final String tableName = bqDataset + "." + destination.replace(".", "_");
    // serialize same destination uploads
    synchronized (uploadLock.computeIfAbsent(destination, k -> new Object())) {
      df.write()
          .mode(saveMode)
          .format(saveFormat)
          .options(saveOptions)
          .option("clusteredFields", clusteringFields)
          .save(tableName);

      numRecords = df.count();
      LOGGER.debug("Uploaded {} rows to:'{}' upload time:{}, ",
          numRecords,
          tableName,
          Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS)
      );
    }

    if (LOGGER.isTraceEnabled()) {
      df.toJavaRDD().foreach(x ->
          LOGGER.trace("uploadDestination df row val:{}", x.toString())
      );
    }
    df.unpersist();

    return numRecords;
  }

}