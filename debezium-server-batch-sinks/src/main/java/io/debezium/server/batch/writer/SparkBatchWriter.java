/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.writer;

import io.debezium.server.batch.BatchUtil;

import java.io.File;
import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public class SparkBatchWriter extends AbstractBatchWriter {

  protected static final Logger LOGGER = LoggerFactory.getLogger(SparkBatchWriter.class);
  private static final String SPARK_PROP_PREFIX = "debezium.sink.sparkbatch.";
  protected final String bucket = ConfigProvider.getConfig().getOptionalValue("debezium.sink.sparkbatch.bucket-name",
      String.class).orElse("s3a://My-S3-Bucket");
  private final SparkConf sparkconf = new SparkConf()
      .setAppName("CDC-Batch-Spark-Sink")
      .setMaster("local");
  protected String saveFormat = ConfigProvider.getConfig().getOptionalValue("debezium.sink.sparkbatch.save-format", String.class).orElse("json");
  SparkSession spark;

  public SparkBatchWriter() {
    super();
    this.initSparkconf();
    this.spark = SparkSession
        .builder()
        .config(this.sparkconf)
        .getOrCreate();

    LOGGER.info("Starting S3 Spark Consumer({})", this.getClass().getName());
    LOGGER.info("Spark save format is '{}'", saveFormat);
  }

  private void initSparkconf() {

    for (String name : ConfigProvider.getConfig().getPropertyNames()) {
      if (name.startsWith(SPARK_PROP_PREFIX)) {
        this.sparkconf.set(name.substring(SPARK_PROP_PREFIX.length()), ConfigProvider.getConfig().getValue(name, String.class));
        LOGGER.info("Setting Spark Conf '{}'='{}'", name.substring(SPARK_PROP_PREFIX.length()), ConfigProvider.getConfig().getValue(name, String.class));
      }
    }
    this.sparkconf.set("spark.ui.enabled", "false");
  }

  @Override
  public void uploadOne(String destination) {
    String s3File = map(destination);

    // Read DF with Schema if schema enabled and exists in the event message
    StructType dfSchema = BatchUtil.getSparkDfSchema(this.getSchema(destination));

    File tempFile = this.getJsonLines(destination);
    if (tempFile == null) {
      return;
    }

    Dataset<Row> df = spark.read().schema(dfSchema).json(tempFile.getAbsolutePath());
    df.write()
        .mode(SaveMode.Append)
        .format(saveFormat)
        .save(bucket + "/" + s3File);
    LOGGER.debug("Saved data to:'{}' rows:{}", s3File, df.count());
    cacheRowCounter.put(destination, 0);
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (!spark.sparkContext().isStopped()) {
      spark.close();
    }
  }
}
