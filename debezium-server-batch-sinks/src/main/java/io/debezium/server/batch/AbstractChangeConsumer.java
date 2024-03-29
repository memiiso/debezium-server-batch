/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.batch.common.InterfaceBatchSizeWait;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public abstract class AbstractChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  protected static final Duration LOG_INTERVAL = Duration.ofMinutes(15);
  protected static final ConcurrentHashMap<String, Object> uploadLock = new ConcurrentHashMap<>();
  protected static final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  protected static final Serde<JsonNode> keySerde = DebeziumSerdes.payloadJson(JsonNode.class);
  static Deserializer<JsonNode> keyDeserializer;
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
  protected static final ObjectMapper mapper = new ObjectMapper();
  protected final Clock clock = Clock.system();
  protected Deserializer<JsonNode> valDeserializer;
  protected long consumerStart = clock.currentTimeInMillis();
  protected long numConsumedEvents = 0;
  protected Threads.Timer logTimer = Threads.timer(clock, LOG_INTERVAL);
  @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
  String valueFormat;
  @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
  String keyFormat;
  @ConfigProperty(name = "debezium.sink.batch.batch-size-wait", defaultValue = "NoBatchSizeWait")
  String batchSizeWaitName;
  @Inject
  @Any
  Instance<InterfaceBatchSizeWait> batchSizeWaitInstances;
  InterfaceBatchSizeWait batchSizeWait;

  public void initizalize() throws InterruptedException {
    // configure and set 
    valSerde.configure(Collections.emptyMap(), false);
    valDeserializer = valSerde.deserializer();
    // configure and set 
    keySerde.configure(Collections.emptyMap(), true);
    keyDeserializer = keySerde.deserializer();

    if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) formats are {json,}!");
    }

    if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.key={" + valueFormat + "} not supported! Supported (debezium.format.key=*) formats are {json,}!");
    }

    batchSizeWait = BatchUtil.selectInstance(batchSizeWaitInstances, batchSizeWaitName);
    LOGGER.info("Using {} to optimize batch size", batchSizeWait.getClass().getSimpleName());
    batchSizeWait.initizalize();
  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {
    LOGGER.trace("Received {} events", records.size());

    Instant start = Instant.now();
    Map<String, List<DebeziumEvent>> events = records.stream()
        .map((ChangeEvent<Object, Object> e)
            -> {
          try {
            return new DebeziumEvent(e.destination(),
                getPayload(e.destination(), e.value()), //valDeserializer.deserialize(e.destination(), getBytes(e.value())),
                e.key() == null ? null : keyDeserializer.deserialize(e.destination(), getBytes(e.key())),
                mapper.readTree(getBytes(e.value())).get("schema"),
                e.key() == null ? null : mapper.readTree(getBytes(e.key())).get("schema")
            );
          } catch (IOException ex) {
            throw new DebeziumException(ex);
          }
        })
        .collect(Collectors.groupingBy(DebeziumEvent::destination));

    long numUploadedEvents = 0;
    for (Map.Entry<String, List<DebeziumEvent>> destinationEvents : events.entrySet()) {
      // group list of events by their schema, if in the batch we have schema change events grouped by their schema
      // so with this uniform schema is guaranteed for each batch
      Map<JsonNode, List<DebeziumEvent>> eventsGroupedBySchema =
          destinationEvents.getValue().stream()
              .collect(Collectors.groupingBy(DebeziumEvent::valueSchema));
      LOGGER.debug("Batch got {} records with {} different schema!!", destinationEvents.getValue().size(),
          eventsGroupedBySchema.keySet().size());

      for (List<DebeziumEvent> schemaEvents : eventsGroupedBySchema.values()) {
        numUploadedEvents += this.uploadDestination(destinationEvents.getKey(), schemaEvents);
      }
    }
    // workaround! somehow offset is not saved to file unless we call committer.markProcessed
    // even its should be saved to file periodically
    for (ChangeEvent<Object, Object> record : records) {
      LOGGER.trace("Processed event '{}'", record);
      committer.markProcessed(record);
    }
    committer.markBatchFinished();
    this.logConsumerProgress(numUploadedEvents);
    LOGGER.debug("Received:{} Processed:{} events", records.size(), numUploadedEvents);

    batchSizeWait.waitMs(numUploadedEvents, (int) Duration.between(start, Instant.now()).toMillis());

  }

  protected void logConsumerProgress(long numUploadedEvents) {
    numConsumedEvents += numUploadedEvents;
    if (logTimer.expired()) {
      LOGGER.info("Consumed {} records after {}", numConsumedEvents, Strings.duration(clock.currentTimeInMillis() - consumerStart));
      numConsumedEvents = 0;
      consumerStart = clock.currentTimeInMillis();
      logTimer = Threads.timer(clock, LOG_INTERVAL);
    }
  }

  public JsonNode getPayload(String destination, Object val) {
    JsonNode pl = valDeserializer.deserialize(destination, getBytes(val));
    // used to partition tables __source_ts
    if (pl.has("__source_ts_ms")) {
      ((ObjectNode) pl).put("__source_ts", pl.get("__source_ts_ms").longValue() / 1000);
    } else {
      ((ObjectNode) pl).put("__source_ts", Instant.now().getEpochSecond());
      ((ObjectNode) pl).put("__source_ts_ms", Instant.now().toEpochMilli());
    }
    return pl;
  }

  protected File getJsonLinesFile(String destination, List<DebeziumEvent> data) {

    Instant start = Instant.now();
    final File tempFile;
    try {
      tempFile = File.createTempFile(UUID.randomUUID() + "-", ".json");
      FileOutputStream fos = new FileOutputStream(tempFile, true);
      LOGGER.debug("Writing {} events as jsonlines file: {}", data.size(), tempFile);

      for (DebeziumEvent e : data) {
        final JsonNode valNode = e.value();

        if (valNode == null) {
          LOGGER.warn("Null Value received skipping the entry! destination:{} key:{}", destination, getString(e.key()));
          continue;
        }

        try {
          final String valData = mapper.writeValueAsString(valNode) + System.lineSeparator();

          fos.write(valData.getBytes(StandardCharsets.UTF_8));
        } catch (IOException ioe) {
          LOGGER.error("Failed writing record to file", ioe);
          fos.close();
          throw new UncheckedIOException(ioe);
        }
      }

      fos.close();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    LOGGER.trace("Writing jsonlines took:{}", Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS));
    return tempFile;
  }

  public abstract long uploadDestination(String destination, List<DebeziumEvent> data) throws InterruptedException;

}
