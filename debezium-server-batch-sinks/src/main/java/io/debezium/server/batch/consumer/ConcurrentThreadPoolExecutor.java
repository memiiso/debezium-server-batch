/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.consumer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.enterprise.context.Dependent;

import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Dependent
public class ConcurrentThreadPoolExecutor {

  protected static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentThreadPoolExecutor.class);
  protected static final ConcurrentHashMap<String, ThreadPoolExecutor> threadPools = new ConcurrentHashMap<>();
  final static Integer uploadThreadNum =
      ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.upload-threads", Integer.class).orElse(1);

  public ConcurrentThreadPoolExecutor() {
    LOGGER.info("Setting concurrent upload number to {}", uploadThreadNum);
  }

  public ThreadPoolExecutor get(String destination) {
    return threadPools.computeIfAbsent(destination, k ->
        new ThreadPoolExecutor(uploadThreadNum, uploadThreadNum, 0L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(12), new ThreadPoolExecutor.DiscardPolicy()));
  }

  public void submit(String destination, Runnable task) {
    this.get(destination).submit(task);
  }

  public void logThredPoolStatus(String destination) {
    ThreadPoolExecutor threadPool = this.get(destination);
    int poolSize = threadPool.getPoolSize();
    int active = threadPool.getActiveCount();
    long submitted = threadPool.getTaskCount();
    long completed = threadPool.getCompletedTaskCount();

    LOGGER.info("Destination:{} upload poolSize:{}, active:{}, waiting:{}, " +
            "total submitted:{}, total completed:{}, not completed:{}", destination,
        poolSize,
        active,
        poolSize - active, submitted,
        completed, submitted - completed);
  }

  public void shutdown() {
    for (Map.Entry<String, ThreadPoolExecutor> entry : threadPools.entrySet()) {
      try {
        this.shutdownQueue(entry.getKey(), entry.getValue());
      } catch (Exception e) {
        LOGGER.warn("Exception during upload queue shutdown, destination '{}'", entry.getKey(), e);
      }
    }
  }

  public void shutdownQueue(String destination, ThreadPoolExecutor threadPool) throws InterruptedException {

    LOGGER.debug("Closing upload queue of destination:{}", destination);
    threadPool.shutdown();
    if (!threadPool.awaitTermination(3, TimeUnit.MINUTES)) {
      LOGGER.warn("Upload queue of destination '{}' did not terminate in the specified time(3m).", destination);
      List<Runnable> droppedTasks = threadPool.shutdownNow();
      LOGGER.warn("Upload queue of destination '{}' was abruptly shut down." +
          " {} tasks will not be executed.", destination, droppedTasks.size());
    } else {
      LOGGER.debug("Closed upload queue of destination '{}'", destination);
    }
  }

}



