/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;

import java.lang.management.ManagementFactory;
import java.util.Optional;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Alternative;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optimizes batch size around 85%-90% of max,batch.size using dynamically calculated sleep(ms)
 *
 * @author Ismail Simsek
 */
@Dependent
@Alternative
public class MaxBatchSizeWait implements InterfaceDynamicWait {
  protected static final Logger LOGGER = LoggerFactory.getLogger(MaxBatchSizeWait.class);
  final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
  @ConfigProperty(name = "debezium.sink.batch.dynamic-wait.snapshot-metrics-mbean", defaultValue = "")
  Optional<String> snapshotMbean;
  @ConfigProperty(name = "debezium.sink.batch.dynamic-wait.streaming-metrics-mbean", defaultValue = "")
  Optional<String> streamingMbean;
  @ConfigProperty(name = "debezium.source.max.queue.size", defaultValue = CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE + "")
  int maxQueueSize;
  @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE + "")
  int maxBatchSize;
  @ConfigProperty(name = "debezium.sink.batch.dynamic-wait.max-wait-ms", defaultValue = "300000")
  int maxWaitMs;
  int numberOfChecks = 20;

  ObjectName snapshotMetricsObjectName;
  ObjectName streamingMetricsObjectName;

  @Override
  public void initizalize() throws DebeziumException {
    assert snapshotMbean.isPresent() :
        "Snapshot metrics Mbean(`debezium.sink.batch.dynamic-wait.snapshot-metrics-mbean`) is not not set";
    assert streamingMbean.isPresent() :
        "Streaming metrics Mbean(`debezium.sink.batch.dynamic-wait.streaming-metrics-mbean`) is not set";

    try {
      snapshotMetricsObjectName = new ObjectName(snapshotMbean.get());
      streamingMetricsObjectName = new ObjectName(streamingMbean.get());
    } catch (Exception e) {
      throw new DebeziumException(e);
    }
  }

  @Override
  public void waitMs(Integer numRecordsProcessed, Integer processingTimeMs) throws InterruptedException {
    printMetrics();

    if (snapshotRunning()) {
      return;
    }

    LOGGER.info("Processed {}, " +
            "QueueCurrentSize:{}, QueueTotalCapacity:{}, SecondsBehindSource:{}, SnapshotCompleted:{}",
        numRecordsProcessed, streamingQueueCurrentSize(), maxQueueSize, streamingMilliSecondsBehindSource() / 1000,
        snapshotCompleted()
    );
    // @TODO use check interval and sum  total instead of numberOfChecks
    for (int i = 0; i < numberOfChecks && streamingQueueCurrentSize() < maxBatchSize; i++) {
      LOGGER.debug("QueueCurrentSize:{} < maxBatchSize:{} Sleeping {} Milliseconds",
          streamingQueueCurrentSize(), maxBatchSize, maxWaitMs / numberOfChecks);
      Thread.sleep(maxWaitMs / numberOfChecks);
    }
  }

  public boolean snapshotRunning() {
    try {
      return (boolean) mbeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotRunning");
    } catch (Exception e) {
      throw new DebeziumException(e);
    }
  }

  public boolean snapshotCompleted() {
    try {
      return (boolean) mbeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotCompleted");
    } catch (Exception e) {
      throw new DebeziumException(e);
    }
  }

  public int streamingQueueTotalCapacity() {
    try {
      return (int) mbeanServer.getAttribute(streamingMetricsObjectName, "QueueTotalCapacity");
    } catch (Exception e) {
      throw new DebeziumException(e);
    }
  }

  public int streamingQueueRemainingCapacity() {
    try {
      return (int) mbeanServer.getAttribute(streamingMetricsObjectName, "QueueRemainingCapacity");
    } catch (Exception e) {
      throw new DebeziumException(e);
    }
  }

  public int streamingQueueCurrentSize() {
    return maxQueueSize - streamingQueueRemainingCapacity();
  }

  public long streamingMilliSecondsBehindSource() {
    try {
      return (long) mbeanServer.getAttribute(streamingMetricsObjectName, "MilliSecondsBehindSource");
    } catch (Exception e) {
      throw new DebeziumException(e);
    }
  }

  public void printMetrics() {
    try {
      LOGGER.info("snapshotRunning {}", snapshotRunning());
      LOGGER.info("snapshotCompleted {}", snapshotCompleted());
      LOGGER.info("streamingQueueCurrentCapacity {}", streamingQueueCurrentSize());
      LOGGER.info("streamingQueueTotalCapacity {}", streamingQueueTotalCapacity());
      LOGGER.info("maxQueueSize {}", maxQueueSize);
      LOGGER.info("streamingQueueRemainingCapacity {}", streamingQueueRemainingCapacity());
      LOGGER.info("streamingMilliSecondsBehindSource {}", streamingMilliSecondsBehindSource());
    } catch (Exception e) {
      throw new DebeziumException("Failed to read metrics", e);
    }
  }

}
