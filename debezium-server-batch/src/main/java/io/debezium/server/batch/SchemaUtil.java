/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.server.batch.batchwriter.AbstractBatchRecordWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public class SchemaUtil {
  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractBatchRecordWriter.class);
  protected static final ObjectMapper jsonObjectMapper = new ObjectMapper();

  public static StructType getSparkDfSchema(JsonNode eventSchema) {

    StructType sparkSchema = new StructType();

    String schemaType = eventSchema.get("type").textValue();
    String schemaName = "root";
    if (eventSchema.has("field")) {
      schemaName = eventSchema.get("field").textValue();
    }
    LOGGER.debug("Converting Schema of: {}::{}", schemaName, schemaType);

    for (JsonNode jsonSchemaFieldNode : eventSchema.get("fields")) {
      String fieldName = jsonSchemaFieldNode.get("field").textValue();
      String fieldType = jsonSchemaFieldNode.get("type").textValue();
      LOGGER.debug("Processing Field: {}.{}::{}", schemaName, fieldName, fieldType);
      // for all the debezium data types please see org.apache.kafka.connect.data.Schema;
      switch (fieldType) {
        case "int8":
        case "int16":
        case "int32":
        case "int64":
          sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.IntegerType, true, Metadata.empty()));
          break;
        case "float8":
        case "float16":
        case "float32":
        case "float64":
          sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.FloatType, true, Metadata.empty()));
          break;
        case "boolean":
          sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.BooleanType, true, Metadata.empty()));
          break;
        case "string":
          sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.StringType, true, Metadata.empty()));
          break;
        case "bytes":
          sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.ByteType, true, Metadata.empty()));
          break;
        case "array":
          sparkSchema = sparkSchema.add(new StructField(fieldName, new ArrayType(), true, Metadata.empty()));
          break;
        case "map":
          sparkSchema = sparkSchema.add(new StructField(fieldName, new MapType(), true, Metadata.empty()));
          break;
        case "struct":
          // recursive call
          StructType subSchema = SchemaUtil.getSparkDfSchema(jsonSchemaFieldNode);
          sparkSchema = sparkSchema.add(new StructField(fieldName, subSchema, true, Metadata.empty()));
          break;
        default:
          // default to String type
          sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.StringType, true, Metadata.empty()));
          break;
      }
    }

    return sparkSchema;

  }

  public static Schema getIcebergSchema(JsonNode eventSchema) {
    LOGGER.debug(eventSchema.toString());
    return getIcebergSchema(eventSchema, "", -1);
  }

  public static Schema getIcebergSchema(JsonNode eventSchema, String schemaName, int columnId) {
    List<Types.NestedField> schemaColumns = new ArrayList<>();
    String schemaType = eventSchema.get("type").textValue();
    LOGGER.debug("Converting Schema of: {}::{}", schemaName, schemaType);
    for (JsonNode jsonSchemaFieldNode : eventSchema.get("fields")) {
      columnId++;
      String fieldName = jsonSchemaFieldNode.get("field").textValue();
      String fieldType = jsonSchemaFieldNode.get("type").textValue();
      LOGGER.debug("Processing Field: [{}] {}.{}::{}", columnId, schemaName, fieldName, fieldType);
      switch (fieldType) {
        case "int8":
        case "int16":
        case "int32": // int 4 bytes
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.IntegerType.get()));
          break;
        case "int64": // long 8 bytes
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.LongType.get()));
          break;
        case "float8":
        case "float16":
        case "float32": // float is represented in 32 bits,
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.FloatType.get()));
          break;
        case "float64": // double is represented in 64 bits
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.DoubleType.get()));
          break;
        case "boolean":
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.BooleanType.get()));
          break;
        case "string":
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.StringType.get()));
          break;
        case "bytes":
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.BinaryType.get()));
          break;
        case "array":
          // @TODO
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.StringType.get()));
          break;
        case "map":
          // @TODO
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.StringType.get()));
          break;
        case "struct":
          // recursive call
          Schema subSchema = SchemaUtil.getIcebergSchema(jsonSchemaFieldNode, fieldName, ++columnId);
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.StructType.of(subSchema.columns())));
          columnId += subSchema.columns().size();
          break;
        default:
          // default to String type
          schemaColumns.add(Types.NestedField.optional(columnId, fieldName, Types.StringType.get()));
          break;
      }
    }
    return new Schema(schemaColumns);
  }

  // @TODO remove!
  public static Schema getEventIcebergSchema(String event) throws JsonProcessingException {
    JsonNode jsonNode = new ObjectMapper().readTree(event);

    if (!SchemaUtil.hasSchema(jsonNode)) {
      return null;
    }
    return SchemaUtil.getIcebergSchema(jsonNode.get("schema"));
  }

  // @TODO remove!
  public static StructType getEventSparkDfSchema(String event) throws JsonProcessingException {
    JsonNode jsonNode = new ObjectMapper().readTree(event);

    if (!SchemaUtil.hasSchema(jsonNode)) {
      return null;
    }
    return SchemaUtil.getSparkDfSchema(jsonNode.get("schema"));
  }

  public static boolean hasSchema(JsonNode jsonNode) {
    return jsonNode != null
        && jsonNode.has("schema")
        && jsonNode.get("schema").has("fields")
        && jsonNode.get("schema").get("fields").isArray();
  }

  public static GenericRecord getIcebergRecord(Schema schema, JsonNode data) {
    // JsonToGenericRecord(Collections.emptyMap(), schema.asStruct().,data);
    Map<String, Object> mappedResult = Collections.emptyMap();
    GenericRecord record = GenericRecord.create(schema);

    schema.
    for (StructType coll : record.struct()) {
      try {
        mappedResult = JsonToGenericRecord(mappedResult, record.struct(), data))
      } catch (IOException e) {
        // @TODO add to signature
      }
    }
    return GenericRecord.create(schema).copy(mappedResult);
  }

  public static Map<String, Object> JsonToGenericRecord(Map<String, Object> mappedResult, Types.NestedField field,
                                                        JsonNode node) throws IOException {
    LOGGER.debug("Converting Field:" + field.name() + " Type:" + field.type());
    System.out.println("Converting Field:" + field.name() + " Type:" + field.type());

    switch (field.type().typeId()) {
      case INTEGER: // int 4 bytes
        mappedResult.put(field.name(), node.asInt());
        break;
      case LONG: // long 8 bytes
        mappedResult.put(field.name(), node.asLong());
        break;
      case FLOAT: // float is represented in 32 bits,
        mappedResult.put(field.name(), node.floatValue());
        break;
      case DOUBLE: // double is represented in 64 bits
        mappedResult.put(field.name(), node.asDouble());
        break;
      case BOOLEAN:
        mappedResult.put(field.name(), node.asBoolean());
        break;
      case STRING:
        mappedResult.put(field.name(), node.asText());
        break;
      case BINARY: // ??? "byte"???
        mappedResult.put(field.name(), node.binaryValue());
        break;
      case LIST:// "array" ???
        // @TODO FIX
        mappedResult.put(field.name(), node.asText());
        break;
      case MAP:
        // @TODO FIX
        mappedResult.put(field.name(), node.asText());
        break;
      case STRUCT:
        // recursive call
        Types.NestedField nField = field;
        mappedResult.put(field.name(), JsonToGenericRecord(Collections.emptyMap(), nField., node.get(field.name())));
        break;
      default:
        // default to String type
        mappedResult.put(field.name(), node.asText());
        break;
    }

    return mappedResult;
  }
}
