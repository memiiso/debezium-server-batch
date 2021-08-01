/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.batch.batchsizewait.InterfaceBatchSizeWait;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
public abstract class AbstractBatchChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
  protected final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  protected final ObjectMapper mapper = new ObjectMapper();
  protected Deserializer<JsonNode> valDeserializer;

  @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
  String valueFormat;

  @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
  String keyFormat;

  @Inject
  InterfaceBatchSizeWait batchSizeWait;

  @Inject
  BeanManager beanManager;

  public static ObjectName getStreamingMetricsObjectName(String connector, String server, String context) throws MalformedObjectNameException {
    return new ObjectName("debezium." + connector + ":type=connector-metrics,context=" + context + ",server=" + server);
  }

  public void initizalize() throws InterruptedException {

    valSerde.configure(Collections.emptyMap(), false);
    valDeserializer = valSerde.deserializer();

    if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) formats are {json,}!");
    }

    if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.key={" + valueFormat + "} not supported! Supported (debezium.format.key=*) formats are {json,}!");
    }

    batchSizeWait.initizalize();

  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {
    LOGGER.debug("Received {} events", records.size());

    Instant start = Instant.now();
    Map<String, ArrayList<ChangeEvent<Object, Object>>> result = records.stream()
        .collect(Collectors.groupingBy(
            ChangeEvent::destination,
            Collectors.mapping(p -> p,
                Collectors.toCollection(ArrayList::new))));

    long numUploadedEvents = 0;
    for (Map.Entry<String, ArrayList<ChangeEvent<Object, Object>>> destinationEvents : result.entrySet()) {
      numUploadedEvents += this.uploadDestination(destinationEvents.getKey(), destinationEvents.getValue());
    }
    // workaround! somehow offset is not saved to file unless we call committer.markProcessed
    // even its should be saved to file periodically
    for (ChangeEvent<Object, Object> record : records) {
      LOGGER.trace("Processed event '{}'", record);
      committer.markProcessed(record);
    }
    committer.markBatchFinished();
    LOGGER.debug("Processed {} events", numUploadedEvents);

    batchSizeWait.waitMs(records.size(), (int) Duration.between(start, Instant.now()).toMillis());

  }

  public JsonlinesBatchFile getJsonLines(String destination, ArrayList<ChangeEvent<Object, Object>> data) {

    Instant start = Instant.now();
    JsonNode valSchema = null;
    JsonNode keySchema = null;
    boolean isFirst = true;
    final File tempFile;
    long numLines = 0L;
    try {
      tempFile = File.createTempFile(UUID.randomUUID() + "-", ".json");
      FileOutputStream fos = new FileOutputStream(tempFile, true);
      LOGGER.debug("Writing {} events as jsonlines file: {}", data.size(), tempFile);

      for (ChangeEvent<Object, Object> e : data) {
        Object val = e.value();
        Object key = e.key();

        // this could happen if multiple threads reading and removing data
        if (val == null) {
          LOGGER.warn("Cache.getJsonLines Null Event Value found for destination:'{}'! " +
              "skipping the entry!", destination);
          continue;
        }
        LOGGER.trace("Cache.getJsonLines val:{}", getString(val));

        if (isFirst) {
          valSchema = BatchUtil.getJsonSchemaNode(getString(val));
          if (key != null) {
            keySchema = BatchUtil.getJsonSchemaNode(getString(key));
          }
          isFirst = false;
        }

        try {
          final JsonNode valNode = valDeserializer.deserialize(destination, getBytes(val));
          final String valData = mapper.writeValueAsString(valNode) + System.lineSeparator();

          if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Cache.getJsonLines val Json Node:{}", valNode.toString());
            LOGGER.trace("Cache.getJsonLines val String:{}", valData);
          }

          fos.write(valData.getBytes(StandardCharsets.UTF_8));
          numLines++;
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

    LOGGER.trace("Writing jsonlines file took:{}",
        Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS));

    // if nothing processed return null
    if (isFirst) {
      tempFile.delete();
      return null;
    }

    return new JsonlinesBatchFile(tempFile, valSchema, keySchema, numLines);
  }

  public abstract long uploadDestination(String destination, ArrayList<ChangeEvent<Object, Object>> data) throws InterruptedException;

}
