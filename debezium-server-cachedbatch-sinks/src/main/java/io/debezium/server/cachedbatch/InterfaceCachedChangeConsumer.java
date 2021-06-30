/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.cachedbatch;

import io.debezium.server.batch.JsonlinesBatchFile;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.enterprise.context.Dependent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Dependent
public interface InterfaceCachedChangeConsumer {

  Logger LOGGER = LoggerFactory.getLogger(InterfaceCachedChangeConsumer.class);
  ScheduledExecutorService timerExecutor = Executors.newSingleThreadScheduledExecutor();

  BatchCache getCache();

  ConcurrentThreadPoolExecutor getThreadPool();

  Integer getBatchInterval();

  Integer getBatchUploadRowLimit();

  long uploadDestination(String destination, JsonlinesBatchFile jsonLines);

  default long startUploadIfRowLimitReached(String destination) {
    // get count per destination
    long cnt = getCache().getEstimatedCacheSize(destination);
    if (cnt < getBatchUploadRowLimit()) {
      return 0L;
    }

    Thread uploadThread = new Thread(() -> {
      Thread.currentThread().setName("spark-row-limit-upload-" + Thread.currentThread().getId());
      // data might be already processed
      if (this.getCache().getEstimatedCacheSize(destination) < getBatchUploadRowLimit()) {
        return;
      }
      LOGGER.debug("Batch row limit reached, cache.size > batchLimit {}>={}, starting upload destination:{}",
          getCache().getEstimatedCacheSize(destination), getBatchUploadRowLimit(), destination);

      this.uploadDestination(destination, this.getCache().getJsonLines(destination));
      getThreadPool().logThredPoolStatus(destination);
      LOGGER.debug("Finished Upload Thread:{}", Thread.currentThread().getName());
    });
    getThreadPool().submit(destination, uploadThread);
    return cnt;
  }

  private void startTimerUpload(String destination) {
    // divide it to batches and upload
    for (int i = 0; i <= (this.getCache().getEstimatedCacheSize(destination) / getBatchUploadRowLimit()) + 1; i++) {

      Thread uploadThread = new Thread(() -> {
        Thread.currentThread().setName("spark-timer-upload-" + Thread.currentThread().getId());
        this.uploadDestination(destination, this.getCache().getJsonLines(destination));
        getThreadPool().logThredPoolStatus(destination);
        LOGGER.debug("Finished Upload Thread:{}", Thread.currentThread().getName());
      });
      getThreadPool().submit(destination, uploadThread);
    }
  }

  default void setupTimerUpload() {
    LOGGER.info("Batch time limit set to {} second", getBatchInterval());
    // Runnable timerTask = () -> {
    Thread timerTask = new Thread(() -> {
      try {
        Thread.currentThread().setName("timer-upload-" + Thread.currentThread().getId());
        LOGGER.info("Timer upload, uploading all cache data(all destinations)!");
        // get cachedestination
        for (String k : getCache().getCaches()) {
          this.startTimerUpload(k);
        }

      } catch (Exception e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
        throw new RuntimeException("Timer based data upload failed!", e);
      }
    });
    timerExecutor.scheduleWithFixedDelay(timerTask, getBatchInterval(), getBatchInterval(), TimeUnit.SECONDS);
  }

  default void stopTimerUpload() {
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

  default void stopUploadQueue() {
    getThreadPool().shutdown();
  }


}
