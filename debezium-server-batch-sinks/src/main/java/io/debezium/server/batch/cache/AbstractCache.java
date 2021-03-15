/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.cache;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.batch.BatchCache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public abstract class AbstractCache implements BatchCache, AutoCloseable {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractCache.class);
  // deserializer
  protected final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  protected final Deserializer<JsonNode> valDeserializer;
  protected final ObjectMapper mapper = new ObjectMapper();
  protected static final ConcurrentHashMap<String, Object> cacheUpdateLock = new ConcurrentHashMap<>();
  final Integer batchRowLimit = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.row-limit", Integer.class).orElse(500);

  public AbstractCache() {
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
  public void close() throws IOException {
    throw new UnsupportedOperationException("Not implemented!");
  }

  @Override
  public void append(String destination, ChangeEvent<Object, Object> record) {
    throw new UnsupportedOperationException("Not implemented!");
  }

  @Override
  public void appendAll(String destination, ArrayList<ChangeEvent<Object, Object>> records) {
    throw new UnsupportedOperationException("Not implemented!");
  }

  @Override
  public BatchJsonlinesFile getJsonLines(String destination) {
    throw new UnsupportedOperationException("Not implemented!");
  }

  @Override
  public Integer getEstimatedCacheSize(String destination) {
    throw new UnsupportedOperationException("Not implemented!");
  }

  @Override
  public Set<String> getCaches() {
    throw new UnsupportedOperationException("Not implemented!");
  }
}



