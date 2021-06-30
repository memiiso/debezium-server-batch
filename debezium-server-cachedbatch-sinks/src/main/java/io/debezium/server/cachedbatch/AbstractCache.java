/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.cachedbatch;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.batch.JsonlinesBatchFile;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
public abstract class AbstractCache implements BatchCache, AutoCloseable {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractCache.class);
  protected static final ConcurrentHashMap<String, Object> cacheUpdateLock = new ConcurrentHashMap<>();
  // deserializer
  protected final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  protected final ObjectMapper mapper = new ObjectMapper();
  protected Deserializer<JsonNode> valDeserializer;
  @ConfigProperty(name = "debezium.sink.batch.row-limit", defaultValue = "500")
  Integer batchRowLimit;

  public AbstractCache() {
  }

  @Override
  public void initialize() {
    valSerde.configure(Collections.emptyMap(), false);
    valDeserializer = valSerde.deserializer();
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
  public abstract void close() throws IOException;

  @Override
  public abstract void appendAll(String destination, List<ChangeEvent<Object, Object>> records);

  @Override
  public abstract JsonlinesBatchFile getJsonLines(String destination);

  @Override
  public abstract Integer getEstimatedCacheSize(String destination);

  @Override
  public abstract Set<String> getCaches();
}

