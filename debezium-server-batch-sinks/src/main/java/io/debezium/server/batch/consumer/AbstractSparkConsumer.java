/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.consumer;

import io.debezium.server.batch.S3StreamNameMapper;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public abstract class AbstractSparkConsumer implements BatchWriter {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractSparkConsumer.class);
  protected static final String SPARK_PROP_PREFIX = "debezium.sink.sparkbatch.";
  protected static final String bucket = ConfigProvider.getConfig().getOptionalValue("debezium.sink.sparkbatch.bucket-name",
      String.class).orElse("s3a://My-S3-Bucket");
  protected final SparkConf sparkconf = new SparkConf()
      .setAppName("CDC-Batch-Spark-Sink")
      .setMaster("local[*]");
  protected final String saveFormat = ConfigProvider.getConfig().getOptionalValue("debezium.sink.sparkbatch.save-format", String.class).orElse("json");
  protected static final ConcurrentHashMap<String, Object> uploadLock = new ConcurrentHashMap<>();
  protected final SparkSession spark;
  @Inject
  protected S3StreamNameMapper s3StreamNameMapper;

  public AbstractSparkConsumer() {
    super();
    this.initSparkconf();
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

  private void initSparkconf() {

    for (String name : ConfigProvider.getConfig().getPropertyNames()) {
      if (name.startsWith(SPARK_PROP_PREFIX)
          && !name.contains("secret") && !name.contains("password") && !name.contains("acess.key")) {
        this.sparkconf.set(name.substring(SPARK_PROP_PREFIX.length()), ConfigProvider.getConfig().getValue(name, String.class));
        //LOGGER.info("Setting Spark Conf '{}'='{}'", name.substring(SPARK_PROP_PREFIX.length()),
        //    ConfigProvider.getConfig().getValue(name, String.class));
      }
    }
    this.sparkconf.set("spark.ui.enabled", "false");
  }

  @Override
  public void close() throws IOException {
    this.stopSparkSession();
  }

}
