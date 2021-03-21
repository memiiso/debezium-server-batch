/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.engine.ChangeEvent;
import io.debezium.server.batch.cache.BatchCache;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("sparkcachedbatch")
@Dependent
public class BatchSparkCachedChangeConsumer extends BatchSparkChangeConsumer {

  protected static final Logger LOGGER = LoggerFactory.getLogger(BatchSparkCachedChangeConsumer.class);

  final ScheduledExecutorService timerExecutor = Executors.newSingleThreadScheduledExecutor();
  @Inject
  protected BatchCache cache;
  @Inject
  protected ConcurrentThreadPoolExecutor threadPool;
  @ConfigProperty(name = "debezium.sink.batch.time-limit", defaultValue = "600")
  Integer batchInterval;
  @ConfigProperty(name = "debezium.sink.batch.row-limit", defaultValue = "10000")
  Integer batchUploadRowLimit;

  @PreDestroy
  void close() {
    try {
      LOGGER.info("Closing batch writer!");
      this.stopTimerUpload();
      this.stopUploadQueue();
      cache.close();
    } catch (Exception e) {
      LOGGER.warn("Exception while closing writer:{} ", e.getMessage());
      e.printStackTrace();
    }
  }

  @PostConstruct
  void connect() throws URISyntaxException, InterruptedException {
    super.connect();
    LOGGER.info("Batch row limit set to {}", batchUploadRowLimit);
    LOGGER.info("Using '{}' as cache", cache.getClass().getSimpleName());
    cache.initialize();
    setupTimerUpload();
  }

  @Override
  public void uploadDestination(String destination, ArrayList<ChangeEvent<Object, Object>> data) throws InterruptedException {
    this.cache.appendAll(destination, data);
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

      this.uploadDestination(destination, this.cache.getJsonLines(destination));
      threadPool.logThredPoolStatus(destination);
      LOGGER.debug("Finished Upload Thread:{}", Thread.currentThread().getName());
    });
    threadPool.submit(destination, uploadThread);
  }

  private void startTimerUpload(String destination) {
    // divide it to batches and upload
    for (int i = 0; i <= (this.cache.getEstimatedCacheSize(destination) / batchUploadRowLimit) + 1; i++) {

      Thread uploadThread = new Thread(() -> {
        Thread.currentThread().setName("spark-timer-upload-" + Thread.currentThread().getId());
        this.uploadDestination(destination, this.cache.getJsonLines(destination));
        threadPool.logThredPoolStatus(destination);
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
