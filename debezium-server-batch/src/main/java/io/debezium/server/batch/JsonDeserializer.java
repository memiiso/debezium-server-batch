/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * JSON deserializer for Jackson's JsonNode tree model. Using the tree model allows it to work with arbitrarily
 * structured data without having associated Java classes. This deserializer also supports Connect schemas.
 */
public class JsonDeserializer implements Deserializer<JsonNode> {
  private final ObjectMapper objectMapper = new ObjectMapper();

  JsonDeserializer() {
    objectMapper.enable(DeserializationFeature.USE_LONG_FOR_INTS);
    objectMapper.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
  }

  @Override
  public JsonNode deserialize(String topic, byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    JsonNode data;
    try {
      data = objectMapper.readTree(bytes);
    } catch (Exception e) {
      throw new SerializationException(e);
    }

    return data;
  }
}
