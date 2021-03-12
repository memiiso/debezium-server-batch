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
import io.debezium.server.batch.cache.BatchJsonlinesFile;
import io.debezium.server.batch.cache.InfinispanCache;
import io.debezium.server.batch.cache.MemoryCache;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
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
  protected static final Boolean partitionData =
      ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.objectkey-partition", Boolean.class).orElse(false);
  protected final BatchCache cache;
  final String objectKeyPrefix = ConfigProvider.getConfig().getValue("debezium.sink.batch.objectkey-prefix", String.class);
  final Integer batchInterval = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.time-limit", Integer.class).orElse(600);
  final Integer batchLimit = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.row-limit", Integer.class).orElse(500);
  final ScheduledExecutorService timerExecutor = Executors.newSingleThreadScheduledExecutor();
  protected static final String cacheStore = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.cache", String.class).orElse("infinispan");
  protected ThreadPoolExecutor threadPool;

  public AbstractConsumer() {
    if (cacheStore.equals("memory")) {
      this.cache = new MemoryCache();
      LOGGER.info("Using ConcurrentHashMap as cache store");
    } else {
      this.cache = new InfinispanCache();
      LOGGER.info("Using Infinispan as cache store");
    }
    setupTimerUpload();
    LOGGER.info("Batch row limit set to {}", batchLimit);
  }

  protected String getPartition() {
    final LocalDateTime batchTime = LocalDateTime.now();
    return "year=" + batchTime.getYear() + "/month=" + StringUtils.leftPad(batchTime.getMonthValue() + "", 2, '0') + "/day="
        + StringUtils.leftPad(batchTime.getDayOfMonth() + "", 2, '0');
  }

  public String map(String destination) {
    Objects.requireNonNull(destination, "destination Cannot be Null");
    if (partitionData) {
      String partitioned = getPartition();
      return objectKeyPrefix + destination + "/" + partitioned;
    } else {
      return objectKeyPrefix + destination;
    }
  }

  @Override
  public void append(String destination, ChangeEvent<Object, Object> record) throws InterruptedException {
    cache.append(destination, record);
    this.startUploadIfRowLimitReached(destination);
  }

  @Override
  public void appendAll(String destination, ArrayList<ChangeEvent<Object, Object>> records) throws InterruptedException {
    cache.appendAll(destination, records);
    this.startUploadIfRowLimitReached(destination);
  }

  @Override
  public BatchJsonlinesFile getJsonLines(String destination) {
    return cache.getJsonLines(destination);
  }


  private void startUploadIfRowLimitReached(String destination) {
    this.startUpload(destination, true);
  }

  private void startTimerUpload(String destination) {
    this.startUpload(destination, false);
  }

  private void startUpload(String destination, boolean checkRowLimit) {

    if (checkRowLimit) {
      // get count per destination
      if (cache.getEstimatedCacheSize(destination) >= batchLimit) {
        LOGGER.debug("Batch row limit reached, cache.size > batchLimit {}>={}, starting upload destination:{}",
            cache.getEstimatedCacheSize(destination), batchLimit, destination);
        // update counter in advance to avoid starting multiple uploads
        this.uploadDestination(destination, "row-limit");
      }

    } else {
      this.uploadDestination(destination, "timer");
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
      LOGGER.info("Stopping timer");
      timerExecutor.shutdown();

      if (!timerExecutor.awaitTermination(3, TimeUnit.MINUTES)) {
        LOGGER.warn("Timer did not terminate in the specified time(3m).");
        List<Runnable> droppedTasks = timerExecutor.shutdownNow();
        LOGGER.warn("Executor was abruptly shut down. " + droppedTasks.size() + " tasks will not be executed.");
      } else {
        LOGGER.info("Stopped timer");
      }
    } catch (Exception e) {
      LOGGER.error("Timer shutdown failed {}", e.getMessage());
    }
  }

  @Override
  public Integer getEstimatedCacheSize(String destination) {
    return cache.getEstimatedCacheSize(destination);
  }

  @Override
  public Set<String> getCaches() {
    return cache.getCaches();
  }
}
