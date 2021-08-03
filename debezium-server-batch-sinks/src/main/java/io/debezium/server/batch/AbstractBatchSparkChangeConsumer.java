/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.DebeziumException;
import io.debezium.server.batch.uploadlock.InterfaceUploadLock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;
import javax.inject.Inject;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public abstract class AbstractBatchSparkChangeConsumer extends AbstractBatchChangeConsumer {

  protected static final String SPARK_PROP_PREFIX = "debezium.sink.sparkbatch.";
  protected static final ConcurrentHashMap<String, Object> uploadLock = new ConcurrentHashMap<>();
  protected final SparkConf sparkconf = new SparkConf()
      .setAppName("CDC-Batch-Spark-Sink")
      .setMaster("local[*]");
  protected SparkSession spark;

  @Inject
  protected ObjectStorageNameMapper objectStorageNameMapper;
  @ConfigProperty(name = "debezium.sink.sparkbatch.bucket-name", defaultValue = "s3a://My-S3-Bucket")
  String bucket;
  @ConfigProperty(name = "debezium.sink.sparkbatch.save-format", defaultValue = "json")
  String saveFormat;
  @ConfigProperty(name = "debezium.sink.sparkbatch.save-mode", defaultValue = "append")
  String saveMode;

  @ConfigProperty(name = "debezium.sink.batch.upload-lock", defaultValue = "NoUploadLock")
  String concurrentUploadLockName;

  @Inject
  @Any
  Instance<InterfaceUploadLock> concurrentUploadLockInstances;
  InterfaceUploadLock concurrentUploadLock;

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

  public void initizalize() throws InterruptedException {
    super.initizalize();

    Map<String, String> appSparkConf = BatchUtil.getConfigSubset(ConfigProvider.getConfig(), SPARK_PROP_PREFIX);
    appSparkConf.forEach(this.sparkconf::set);
    this.sparkconf.set("spark.ui.enabled", "false");

    LOGGER.info("Creating Spark session");
    this.spark = SparkSession
        .builder()
        .config(this.sparkconf)
        .getOrCreate();

    Instance<InterfaceUploadLock> instance = concurrentUploadLockInstances.select(NamedLiteral.of(concurrentUploadLockName));
    if (instance.isAmbiguous()) {
      throw new DebeziumException("Multiple upload lock class named '" + concurrentUploadLockName + "' were found");
    } else if (instance.isUnsatisfied()) {
      throw new DebeziumException("No batch upload lock class named '" + concurrentUploadLockName + "' is available");
    }
    concurrentUploadLock = instance.get();

    LOGGER.info("Starting Spark Consumer({})", this.getClass().getSimpleName());
    LOGGER.info("Spark save format is '{}'", saveFormat);
    LOGGER.info("Spark Version {}", this.spark.version());
    LOGGER.info("Spark Config Values\n{}", this.spark.sparkContext().getConf().toDebugString());
    LOGGER.info("Using {}", concurrentUploadLock.getClass().getName());
  }

}
