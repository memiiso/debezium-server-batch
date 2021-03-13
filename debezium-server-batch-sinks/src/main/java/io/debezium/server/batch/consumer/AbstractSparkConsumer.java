/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.consumer;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

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
public abstract class AbstractSparkConsumer extends AbstractConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractSparkConsumer.class);

  protected static final String SPARK_PROP_PREFIX = "debezium.sink.sparkbatch.";

  @ConfigProperty(name = "debezium.sink.sparkbatch.bucket-name", defaultValue = "s3a://My-S3-Bucket")
  String bucket;

  protected final SparkConf sparkconf = new SparkConf()
      .setAppName("CDC-Batch-Spark-Sink")
      .setMaster("local[*]");

  @ConfigProperty(name = "debezium.sink.sparkbatch.save-format", defaultValue = "json")
  String saveFormat;

  protected static final ConcurrentHashMap<String, Object> uploadLock = new ConcurrentHashMap<>();

  protected final SparkSession spark;

  public AbstractSparkConsumer() {
    super();
    this.initSparkconf();
    LOG.info("Creating Spark session");
    this.spark = SparkSession
        .builder()
        .config(this.sparkconf)
        .getOrCreate();

    LOG.info("Spark Config Values\n{}", this.spark.sparkContext().getConf().toDebugString());

  }

  protected void stopSparkSession() {
    try {
      LOG.info("Closing Spark");
      if (!spark.sparkContext().isStopped()) {
        spark.close();
      }
      LOG.debug("Closed Spark");
    } catch (Exception e) {
      LOG.warn("Exception during Spark shutdown ", e);
    }
  }

  private void initSparkconf() {

    for (String name : ConfigProvider.getConfig().getPropertyNames()) {
      if (name.startsWith(SPARK_PROP_PREFIX)
          && !name.contains("secret") && !name.contains("password") && !name.contains("acess.key")) {
        this.sparkconf.set(name.substring(SPARK_PROP_PREFIX.length()), ConfigProvider.getConfig().getValue(name, String.class));
        //LOG.info("Setting Spark Conf '{}'='{}'", name.substring(SPARK_PROP_PREFIX.length()),
        //    ConfigProvider.getConfig().getValue(name, String.class));
      }
    }
    this.sparkconf.set("spark.ui.enabled", "false");
  }

  @Override
  public void close() throws IOException {
    this.stopTimerUpload();
    this.stopUploadQueue();
    this.stopSparkSession();
    cache.close();

  }

}
