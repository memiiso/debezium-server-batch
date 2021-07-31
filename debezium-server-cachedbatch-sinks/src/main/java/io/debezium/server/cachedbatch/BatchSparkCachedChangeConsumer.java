/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.cachedbatch;

import io.debezium.engine.ChangeEvent;
import io.debezium.server.batch.BatchSparkChangeConsumer;
import io.debezium.server.batch.JsonlinesBatchFile;

import java.util.ArrayList;
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

  @Inject
  protected BatchCache cache;
  @Inject
  protected ConcurrentThreadPoolExecutor threadPool;
  Logger LOGGER = LoggerFactory.getLogger(this.getClass());
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
      this.cache.close();
      this.stopSparkSession();
    } catch (Exception e) {
      LOGGER.warn("Exception while closing writer:{} ", e.getMessage());
      e.printStackTrace();
    }
  }

  @PostConstruct
  void connect() throws InterruptedException {
    super.initizalize();
    LOGGER.info("Batch row limit set to {}", batchUploadRowLimit);
    LOGGER.info("Using '{}' as cache", cache.getClass().getSimpleName());
    cache.initialize();
    setupTimerUpload();
  }

  @Override
  public long uploadDestination(String destination, ArrayList<ChangeEvent<Object, Object>> data) throws InterruptedException {
    this.cache.appendAll(destination, data);
    return this.startUploadIfRowLimitReached(destination);
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
  public long uploadDestination(String destination, JsonlinesBatchFile jsonLines) throws InterruptedException {
    return super.uploadDestination(destination, jsonLines);
  }
}
