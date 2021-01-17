/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.serde.DebeziumSerdes;
import io.debezium.util.Testing;

import java.util.Collections;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

class TestBatchUtil {

  protected static final Logger LOGGER = LoggerFactory.getLogger(TestBatchUtil.class);
  final String serdeUpdate = Testing.Files.readResourceAsString("json/serde-update.json");
  final String serdeWithSchema = Testing.Files.readResourceAsString("json/serde-with-schema.json");
  final String serdeWithSchema2 = Testing.Files.readResourceAsString("json/serde-with-schema2.json");
  final String unwrapWithSchema = Testing.Files.readResourceAsString("json/unwrap-with-schema.json");

  public StructType getEventSparkDfSchema(String event) throws JsonProcessingException {
    JsonNode jsonNode = new ObjectMapper().readTree(event);

    if (!BatchUtil.hasSchema(jsonNode)) {
      return null;
    }
    return BatchUtil.getSparkDfSchema(jsonNode.get("schema"));
  }

  @Test
  public void testSimpleSchema() throws JsonProcessingException {
    StructType s = getEventSparkDfSchema(unwrapWithSchema);
    assertNotNull(s);
    assertTrue(s.catalogString().contains("id:int,order_date:int,purchaser:int,quantity:int,product_id:int,__op:string"));
  }

  @Test
  public void testNestedSparkSchema() throws JsonProcessingException {
    StructType s = getEventSparkDfSchema(serdeWithSchema);
    assertNotNull(s);
    assertTrue(s.catalogString().contains("before:struct<id"));
    assertTrue(s.catalogString().contains("after:struct<id"));
  }

  @Test
  public void valuePayloadWithSchemaAsJsonNode() {
    // testing Debezium deserializer
    final Serde<JsonNode> valueSerde = DebeziumSerdes.payloadJson(JsonNode.class);
    valueSerde.configure(Collections.emptyMap(), false);
    JsonNode deserializedData = valueSerde.deserializer().deserialize("xx", serdeWithSchema.getBytes());
    System.out.println(deserializedData.getClass().getSimpleName());
    System.out.println(deserializedData.has("payload"));
    assertEquals(deserializedData.getClass().getSimpleName(), "ObjectNode");
    System.out.println(deserializedData);
    assertTrue(deserializedData.has("after"));
    assertTrue(deserializedData.has("op"));
    assertTrue(deserializedData.has("before"));
    assertFalse(deserializedData.has("schema"));

    valueSerde.configure(Collections.singletonMap("from.field", "schema"), false);
    JsonNode deserializedSchema = valueSerde.deserializer().deserialize("xx", serdeWithSchema.getBytes());
    System.out.println(deserializedSchema);
    assertFalse(deserializedSchema.has("schema"));
  }

}
