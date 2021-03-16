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
import io.debezium.server.batch.writer.BatchWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

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
@Named("batch")
@Dependent
public class BatchChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  protected static final Logger LOGGER = LoggerFactory.getLogger(BatchChangeConsumer.class);
  protected final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  protected final ObjectMapper mapper = new ObjectMapper();
  protected Deserializer<JsonNode> valDeserializer;

  @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
  String valueFormat;

  @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
  String keyFormat;

  @Inject
  BatchWriter batchWriter;

  @PreDestroy
  void close() {
    try {
      LOGGER.info("Closing spark!");
      batchWriter.close();
    } catch (Exception e) {
      LOGGER.warn("Exception while closing spark:{} ", e.getMessage());
      e.printStackTrace();
    }
  }

  @PostConstruct
  void connect() throws URISyntaxException, InterruptedException {

    LOGGER.info("Using '{}' batch writer", batchWriter.getClass().getName());
    batchWriter.initialize();

    valSerde.configure(Collections.emptyMap(), false);
    valDeserializer = valSerde.deserializer();

    if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) formats are {json,}!");
    }
    if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.key={" + valueFormat + "} not supported! Supported (debezium.format.key=*) formats are {json,}!");
    }
  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {

    Map<String, ArrayList<ChangeEvent<Object, Object>>> result = records.stream()
        .collect(Collectors.groupingBy(
            ChangeEvent::destination,
            Collectors.mapping(p -> p,
                Collectors.toCollection(ArrayList::new))));

    for (Map.Entry<String, ArrayList<ChangeEvent<Object, Object>>> destinationEvents : result.entrySet()) {
      batchWriter.uploadDestination(destinationEvents.getKey(),
          getJsonLines(destinationEvents.getKey(), destinationEvents.getValue()));
    }
    // workaround! somehow offset is not saved to file unless we call committer.markProcessed
    // even its should be saved to file periodically
    if (!records.isEmpty()) {
      committer.markProcessed(records.get(0));
    }
    committer.markBatchFinished();
  }

  public BatchJsonlinesFile getJsonLines(String destination, ArrayList<ChangeEvent<Object, Object>> data) {

    JsonNode schema = null;
    boolean isFirst = true;
    final File tempFile;
    try {
      tempFile = File.createTempFile(UUID.randomUUID() + "-", ".json");
      FileOutputStream fos = new FileOutputStream(tempFile, true);

      for (ChangeEvent<Object, Object> e : data) {
        Object val = e.value();

        // this could happen if multiple threads reading and removing data
        if (val == null) {
          LOGGER.warn("Cache.getJsonLines Null Event Value found for destination:'{}'! " +
              "skipping the entry!", destination);
          continue;
        }
        LOGGER.trace("Cache.getJsonLines val:{}", getString(val));

        if (isFirst) {
          schema = BatchUtil.getJsonSchemaNode(getString(val));
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

    // if nothing processed return null
    if (isFirst) {
      tempFile.delete();
      return null;
    }

    return new BatchJsonlinesFile(tempFile, schema);
  }

}
