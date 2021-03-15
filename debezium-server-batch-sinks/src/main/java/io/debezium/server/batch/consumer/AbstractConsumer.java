/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.consumer;

import io.debezium.engine.ChangeEvent;
import io.debezium.server.batch.BatchCache;
import io.debezium.server.batch.BatchWriter;
import io.debezium.server.batch.S3StreamNameMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public abstract class AbstractConsumer implements BatchWriter {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractConsumer.class);
  @Inject
  protected BatchCache cache;
  final Integer batchInterval = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.time-limit", Integer.class).orElse(600);
  final Integer batchUploadRowLimit = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.row-limit", Integer.class).orElse(500);
  final ScheduledExecutorService timerExecutor = Executors.newSingleThreadScheduledExecutor();
  protected static final String cacheStore = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.cache", String.class).orElse("infinispan");
  @Inject
  protected ConcurrentThreadPoolExecutor threadPool;
  @Inject
  protected S3StreamNameMapper s3StreamNameMapper;

  public AbstractConsumer() {
    setupTimerUpload();
    LOGGER.info("Batch row limit set to {}", batchUploadRowLimit);
  }

  @Override
  public void appendAll(String destination, ArrayList<ChangeEvent<Object, Object>> records) throws InterruptedException {
    cache.appendAll(destination, records);
    this.startUploadIfRowLimitReached(destination);
  }

  private void startUploadIfRowLimitReached(String destination) {
    // get count per destination
    if (cache.getEstimatedCacheSize(destination) < batchUploadRowLimit) {
      return;
    }

    Thread uploadThread = new Thread(() -> {
      Thread.currentThread().setName("spark-row-limit-upload-" + Thread.currentThread().getId());
      // data might be already processed
      if (this.cache.getEstimatedCacheSize(destination) < batchUploadRowLimit) {
        return;
      }
      LOGGER.debug("Batch row limit reached, cache.size > batchLimit {}>={}, starting upload destination:{}",
          cache.getEstimatedCacheSize(destination), batchUploadRowLimit, destination);

      this.uploadDestination(destination);
      LOGGER.debug("Finished Upload Thread:{}", Thread.currentThread().getName());
    });
    threadPool.submit(destination, uploadThread);
  }

  private void startTimerUpload(String destination) {
    // divide it to batches and upload
    for (int i = 0; i <= (this.cache.getEstimatedCacheSize(destination) / batchUploadRowLimit) + 1; i++) {

      Thread uploadThread = new Thread(() -> {
        Thread.currentThread().setName("spark-timer-upload-" + Thread.currentThread().getId());
        this.uploadDestination(destination);
        LOGGER.debug("Finished Upload Thread:{}", Thread.currentThread().getName());
      });
      threadPool.submit(destination, uploadThread);

    }
  }

  protected void setupTimerUpload() {
    LOGGER.info("Batch time limit set to {} second", batchInterval);
    // Runnable timerTask = () -> {
    Thread timerTask = new Thread(() -> {
      try {
        Thread.currentThread().setName("timer-upload-" + Thread.currentThread().getId());
        LOGGER.info("Timer upload, uploading all cache data(all destinations)!");
        // get cachedestination
        for (String k : cache.getCaches()) {
          this.startTimerUpload(k);
        }

      } catch (Exception e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
        throw new RuntimeException("Timer based data upload failed!", e);
      }
    });
    timerExecutor.scheduleWithFixedDelay(timerTask, batchInterval, batchInterval, TimeUnit.SECONDS);
  }

  protected void stopTimerUpload() {
    try {
      LOGGER.info("Stopping timer task");
      timerExecutor.shutdown();

      if (!timerExecutor.awaitTermination(3, TimeUnit.MINUTES)) {
        LOGGER.warn("Timer did not terminate in the specified time(3m).");
        List<Runnable> droppedTasks = timerExecutor.shutdownNow();
        LOGGER.warn("Executor was abruptly shut down. " + droppedTasks.size() + " tasks will not be executed.");
      } else {
        LOGGER.debug("Stopped timer");
      }
    } catch (Exception e) {
      LOGGER.error("Timer shutdown failed {}", e.getMessage());
    }
  }

  protected void stopUploadQueue() {
    threadPool.shutdown();
  }

}
