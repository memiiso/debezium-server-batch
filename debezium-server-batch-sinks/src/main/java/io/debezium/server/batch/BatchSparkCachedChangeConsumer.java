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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
public class BatchSparkCachedChangeConsumer extends BatchSparkChangeConsumer implements InterfaceCachedChangeConsumer {

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

  @Override
  public BatchCache getCache() {
    return cache;
  }

  @Override
  public ConcurrentThreadPoolExecutor getThreadPool() {
    return threadPool;
  }

  @Override
  public Integer getBatchInterval() {
    return batchInterval;
  }

  @Override
  public Integer getBatchUploadRowLimit() {
    return batchUploadRowLimit;
  }

  @Override
  public void uploadDestination(String destination, JsonlinesBatchFile jsonLines) {
    super.uploadDestination(destination, jsonLines);
  }
}
