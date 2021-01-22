/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.writer;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.batch.BatchUtil;
import io.debezium.server.batch.BatchWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.ConfigProvider;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public abstract class AbstractBatchWriter implements BatchWriter, AutoCloseable {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractBatchWriter.class);
  final String objectKeyPrefix = ConfigProvider.getConfig().getValue("debezium.sink.batch.objectkey-prefix", String.class);
  final String cacheDir = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.cache-dir",
      String.class).orElse("./cache");
  final Integer batchLimit = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.row.limit", Integer.class).orElse(500);
  final Integer batchInterval = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.time.limit", Integer.class).orElse(600);
  final ScheduledExecutorService timerExecutor = Executors.newSingleThreadScheduledExecutor();
  protected LocalDateTime batchTime = LocalDateTime.now();
  protected Boolean partitionData =
      ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.objectkey-partition", Boolean.class).orElse(false);
  protected Boolean schemaEnabled =
      ConfigProvider.getConfig().getOptionalValue("debezium.format.value.schemas.enable", Boolean.class).orElse(false);
  ConcurrentHashMap<String, Integer> cacheRowCounter = new ConcurrentHashMap<>();
  DefaultCacheManager cm = new DefaultCacheManager();
  ConfigurationBuilder builder = new ConfigurationBuilder().simpleCache(true);
  // deserializer
  Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  Deserializer<JsonNode> valDeserializer;
  ObjectMapper mapper = new ObjectMapper();

  public AbstractBatchWriter() {
    cm.start();
    valSerde.configure(Collections.emptyMap(), false);
    valDeserializer = valSerde.deserializer();
    LOGGER.info("Batch row limit set to {}", batchLimit);

    // DISABLED! this can be achieved using poll.interval.ms and max.batch.size
// poll.interval.ms = Positive integer value that specifies the number of milliseconds the connector should wait during each iteration for new change events to appear. Defaults to 1000 milliseconds,
// or 1 second.
    setupTimer();
  }

  protected void setupTimer() {
    LOGGER.info("Batch time limit set to {} second", batchInterval);
    Runnable timerTask = () -> {
      try {
        this.uploadAll();
      } catch (Exception e) {
        LOGGER.error("Timer based batch upload failed!");
      }
    };
    timerExecutor.scheduleWithFixedDelay(timerTask, batchInterval, batchInterval, TimeUnit.SECONDS);
  }

  protected void stopTimer() {
    if (timerExecutor.isShutdown()) {
      return;
    }

    try {
      if (!timerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        timerExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      LOGGER.error("Timer Shutting Down Failed {}", e.getMessage());
      timerExecutor.shutdownNow();
    }
  }

  protected byte[] getBytes(Object object) {
    if (object instanceof byte[]) {
      return (byte[]) object;
    } else if (object instanceof String) {
      return ((String) object).getBytes();
    }
    throw new DebeziumException(unsupportedTypeMessage(object));
  }

  protected String getString(Object object) {
    if (object instanceof String) {
      return (String) object;
    }
    throw new DebeziumException(unsupportedTypeMessage(object));
  }

  protected String unsupportedTypeMessage(Object object) {
    final String type = (object == null) ? "null" : object.getClass().getName();
    return "Unexpected data type '" + type + "'";
  }

  @Override
  public void append(ChangeEvent<Object, Object> record) {

    final String destination = record.destination();

    // create cache for the table if not exists
    if (!cm.cacheExists(destination)) {
      cm.defineConfiguration(destination, builder.build());
      cacheRowCounter.put(destination, 0);
    }
    // append event/record to it
    cm.getCache(destination).put(UUID.randomUUID().toString(), record.value());
    cacheRowCounter.put(destination, cacheRowCounter.getOrDefault(destination, 0) + 1);

    if (cacheRowCounter.getOrDefault(destination, 0) >= batchLimit) {
      LOGGER.debug("Batch Row Limit reached Uploading Data, destination:{}", destination);
      this.uploadOne(destination);
      cacheRowCounter.put(destination, 0);
    }
  }

  // move this to separate interface batch writer
  @Override
  public void uploadOne(String destination) {
    throw new NotImplementedException("Not Implemented!");
  }

  @Override
  public void uploadAll() {
    int numBatchFiles = 0;
    batchTime = LocalDateTime.now();
    for (String k : cm.getCacheConfigurationNames()) {
      uploadOne(k);
      numBatchFiles++;
    }
    LOGGER.info("Uploaded '{}' files.", numBatchFiles);
  }

  @Override
  public void close() throws IOException {

    for (String k : cm.getCacheConfigurationNames()) {
      if (cm.isRunning(k)) {
        LOGGER.warn("Close called, uploading cache data for destination '{}' and closing cache.", k);
        this.uploadOne(k);
        cm.getCache(k).stop();
      }
    }
    cm.close();
    stopTimer();
  }

  protected String getPartition() {
    return "year=" + batchTime.getYear() + "/month=" + StringUtils.leftPad(batchTime.getMonthValue() + "", 2, '0') + "/day="
        + StringUtils.leftPad(batchTime.getDayOfMonth() + "", 2, '0');
  }

  public String map(String destination) {
    Objects.requireNonNull(destination, "destination Cannot be Null");
    Objects.requireNonNull(batchTime, "batchTime Cannot be Null");
    if (partitionData) {
      String partitioned = getPartition();
      return objectKeyPrefix + destination + "/" + partitioned;
    } else {
      return objectKeyPrefix + destination;
    }
  }

  protected File getJsonLines(String destination) {
    Cache<Object, Object> cache = cm.getCache(destination);
    File tempFile;

    if (!(cache.size() > 0)) {
      return null;
    }

    try {
      tempFile = File.createTempFile(UUID.randomUUID() + "-", ".json");
      FileOutputStream fos = new FileOutputStream(tempFile, true);
      for (Object k : cache.keySet()) {
        Object val = cache.remove(k);
        fos.write(mapper.writeValueAsString(valDeserializer.deserialize(destination, getBytes(val))).getBytes(StandardCharsets.UTF_8));
        fos.write(System.lineSeparator().getBytes(StandardCharsets.UTF_8));
      }
      fos.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    return tempFile;
  }

  // @TODO add unit test
  protected JsonNode getSchema(String destination) {
    Cache<Object, Object> cache = cm.getCache(destination);

    if (!(cache.size() > 0 && schemaEnabled)) {
      return null;
    }

    try {
      Optional<Object> eventVal = cache.values().stream().findAny();
      JsonNode jsonNode = new ObjectMapper().readTree(getString(eventVal.get()));

      if (BatchUtil.hasSchema(jsonNode)) {
        return jsonNode.get("schema");
      }

    } catch (Exception e) {
      // pass
    }

    return null;
  }

}


