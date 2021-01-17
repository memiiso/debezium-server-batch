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
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

class TestSchemaUtil {

  protected static final Logger LOGGER = LoggerFactory.getLogger(TestSchemaUtil.class);
  final String serdeUpdate = Testing.Files.readResourceAsString("json/serde-update.json");
  final String serdeWithSchema = Testing.Files.readResourceAsString("json/serde-with-schema.json");
  final String serdeWithSchema2 = Testing.Files.readResourceAsString("json/serde-with-schema2.json");
  final String unwrapWithSchema = Testing.Files.readResourceAsString("json/unwrap-with-schema.json");

  public StructType getEventSparkDfSchema(String event) throws JsonProcessingException {
    JsonNode jsonNode = new ObjectMapper().readTree(event);

    if (!SchemaUtil.hasSchema(jsonNode)) {
      return null;
    }
    return SchemaUtil.getSparkDfSchema(jsonNode.get("schema"));
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
  public void testNestedIcebergSchema() throws JsonProcessingException {
    Schema s = SchemaUtil.getEventIcebergSchema(serdeWithSchema);
    // StructType ss = ConsumerUtil.getEventSparkDfSchema(serdeWithSchema);
    assertNotNull(s);
    assertEquals(s.findField("ts_ms").fieldId(), 29);
    assertEquals(s.findField(7).name(), "after");
    assertTrue(s.asStruct().toString().contains("source: optional struct<"));
    assertTrue(s.asStruct().toString().contains("after: optional struct<"));
    s = SchemaUtil.getEventIcebergSchema(unwrapWithSchema);
  }

  @Test
  public void testNestedIcebergSchema2() throws JsonProcessingException {
    Schema s = SchemaUtil.getIcebergSchema(new ObjectMapper().readTree(serdeWithSchema2));
    // assertEquals(s.asStruct().toString(), "xx");
    assertTrue(s.asStruct().toString().contains("source: optional struct<"));
    assertTrue(s.asStruct().toString().contains("after: optional struct<"));
  }

  @Test
  public void testNestedJsonRecord() throws JsonProcessingException {
    JsonNode event = new ObjectMapper().readTree(serdeWithSchema).get("payload");
    Schema schema = SchemaUtil.getEventIcebergSchema(serdeWithSchema);

    assert schema != null;
    GenericRecord record = SchemaUtil.getIcebergRecord(schema, event);
    System.out.println(record.getField("after").toString());
    System.out.println(record.getField("after").getClass().getSimpleName());
    System.out.println(record.getClass().getSimpleName());

    // unwrapped
    JsonNode eventUw = new ObjectMapper().readTree(unwrapWithSchema).get("payload");
    Schema schemaUw = SchemaUtil.getEventIcebergSchema(unwrapWithSchema);
    GenericRecord recordUw = SchemaUtil.getIcebergRecord(schemaUw, eventUw);

    fail();
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
