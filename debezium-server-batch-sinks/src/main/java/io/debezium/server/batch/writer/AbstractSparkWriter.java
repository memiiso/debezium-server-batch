/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.writer;

import io.debezium.server.batch.BatchUtil;
import io.debezium.server.batch.ObjectStorageNameMapper;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public abstract class AbstractSparkWriter implements BatchWriter {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractSparkWriter.class);
  protected static final String SPARK_PROP_PREFIX = "debezium.sink.sparkbatch.";

  @ConfigProperty(name = "debezium.sink.sparkbatch.bucket-name", defaultValue = "s3a://My-S3-Bucket")
  String bucket;

  protected final SparkConf sparkconf = new SparkConf()
      .setAppName("CDC-Batch-Spark-Sink")
      .setMaster("local[*]");

  @ConfigProperty(name = "debezium.sink.sparkbatch.save-format", defaultValue = "json")
  String saveFormat;

  @ConfigProperty(name = "debezium.sink.sparkbatch.save-mode", defaultValue = "append")
  String saveMode;

  protected static final ConcurrentHashMap<String, Object> uploadLock = new ConcurrentHashMap<>();
  protected SparkSession spark;
  @Inject
  protected ObjectStorageNameMapper objectStorageNameMapper;

  public AbstractSparkWriter() {

  }

  @Override
  public void initialize() {
    Map<String, String> appSparkConf = BatchUtil.getConfigSubset(ConfigProvider.getConfig(), SPARK_PROP_PREFIX);
    appSparkConf.forEach(this.sparkconf::set);
    this.sparkconf.set("spark.ui.enabled", "false");

    LOGGER.info("Creating Spark session");
    this.spark = SparkSession
        .builder()
        .config(this.sparkconf)
        .getOrCreate();

    LOGGER.info("Spark Config Values\n{}", this.spark.sparkContext().getConf().toDebugString());
  }

  protected void stopSparkSession() {
    try {
      LOGGER.info("Closing Spark");
      if (!spark.sparkContext().isStopped()) {
        spark.close();
      }
      LOGGER.debug("Closed Spark");
    } catch (Exception e) {
      LOGGER.warn("Exception during Spark shutdown ", e);
    }
  }

  @Override
  public void close() throws IOException {
    this.stopSparkSession();
  }

}
