/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.consumer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
public abstract class AbstractSparkConsumer extends AbstractConsumer {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractSparkConsumer.class);
  protected static final String SPARK_PROP_PREFIX = "debezium.sink.sparkbatch.";
  protected static final String bucket = ConfigProvider.getConfig().getOptionalValue("debezium.sink.sparkbatch.bucket-name",
      String.class).orElse("s3a://My-S3-Bucket");
  protected final SparkConf sparkconf = new SparkConf()
      .setAppName("CDC-Batch-Spark-Sink")
      .setMaster("local[*]");
  protected final String saveFormat = ConfigProvider.getConfig().getOptionalValue("debezium.sink.sparkbatch.save-format", String.class).orElse("json");
  protected static final ConcurrentHashMap<String, Object> uploadLock = new ConcurrentHashMap<>();
  final static Integer uploadThreadNum =
      ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.upload-threads", Integer.class).orElse(1);
  protected final ThreadPoolExecutor threadPool;
  protected final SparkSession spark;

  public AbstractSparkConsumer() {
    super();
    this.initSparkconf();
    LOGGER.info("Creating Spark session");
    this.spark = SparkSession
        .builder()
        .config(this.sparkconf)
        .getOrCreate();

    LOGGER.info("Spark Config Values\n{}", this.spark.sparkContext().getConf().toDebugString());
    LOGGER.info("Setting concurrent upload number to {}", uploadThreadNum);
    threadPool = new ThreadPoolExecutor(uploadThreadNum, uploadThreadNum, 0L, TimeUnit.SECONDS,
        new SynchronousQueue<>(), new ThreadPoolExecutor.DiscardPolicy());

  }

  protected void stopUploadQueue() {
    try {
      LOGGER.info("Closing upload queue");
      threadPool.shutdown();

      if (!threadPool.awaitTermination(3, TimeUnit.MINUTES)) {
        LOGGER.warn("Upload queue did not terminate in the specified time(3m).");
        List<Runnable> droppedTasks = threadPool.shutdownNow();
        LOGGER.warn("Upload queue was abruptly shut down. " + droppedTasks.size() + " tasks will not be executed.");
      } else {
        LOGGER.info("Closed upload queue");
      }
    } catch (Exception e) {
      LOGGER.warn("Exception during upload queue shutdown ", e);
    }
  }


  protected void stopSparkSession() {
    try {
      LOGGER.info("Closing Spark");
      if (!spark.sparkContext().isStopped()) {
        spark.close();
      }
      LOGGER.info("Closed Spark");
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
    this.stopTimerUpload();
    this.stopUploadQueue();
    this.stopSparkSession();
    cache.close();

  }

}
