/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.*;
import org.apache.spark.sql.types.*;
import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public class BatchUtil {
  public static final ObjectMapper jsonObjectMapper = new ObjectMapper();
  protected static final Logger LOGGER = LoggerFactory.getLogger(BatchUtil.class);

  public static StructType getSparkDfSchema(JsonNode eventSchema) {

    if (eventSchema == null) {
      return null;
    }

    StructType sparkSchema = new StructType();

    String schemaType = eventSchema.get("type").textValue();
    String schemaName = "root";
    if (eventSchema.has("field")) {
      schemaName = eventSchema.get("field").textValue();
    }
    LOGGER.trace("Converting Schema of: {}::{}", schemaName, schemaType);

    for (JsonNode jsonSchemaFieldNode : eventSchema.get("fields")) {
      String fieldName = jsonSchemaFieldNode.get("field").textValue();
      String fieldType = jsonSchemaFieldNode.get("type").textValue();
      LOGGER.trace("Processing Field: {}.{}::{}", schemaName, fieldName, fieldType);
      // for all the debezium data types please see org.apache.kafka.connect.data.Schema;
      switch (fieldType) {
        case "int8":
        case "int16":
        case "int32":
          sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.IntegerType, true, Metadata.empty()));
          break;
        case "int64":
          sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.LongType, true, Metadata.empty()));
          break;
        case "float8":
        case "float16":
        case "float32":
          sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.FloatType, true, Metadata.empty()));
          break;
        case "float64":
          sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.DoubleType, true, Metadata.empty()));
          break;
        case "boolean":
          sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.BooleanType, true, Metadata.empty()));
          break;
        case "string":
          sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.StringType, true, Metadata.empty()));
          break;
        case "bytes":
          sparkSchema = sparkSchema.add(new StructField(fieldName, DataTypes.BinaryType, true, Metadata.empty()));
          break;
        case "array":
          sparkSchema = sparkSchema.add(new StructField(fieldName, new ArrayType(), true, Metadata.empty()));
          break;
        case "map":
          sparkSchema = sparkSchema.add(new StructField(fieldName, new MapType(), true, Metadata.empty()));
          break;
        case "struct":
          // recursive call
          StructType subSchema = BatchUtil.getSparkDfSchema(jsonSchemaFieldNode);
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

  public static boolean hasSchema(JsonNode jsonNode) {
    return jsonNode != null
        && jsonNode.has("schema")
        && jsonNode.get("schema").has("fields")
        && jsonNode.get("schema").get("fields").isArray();
  }


  public static JsonNode getSchemaNode(String eventVal) {

    try {
      JsonNode jsonNode = BatchUtil.jsonObjectMapper.readTree(eventVal);

      if (BatchUtil.hasSchema(jsonNode)) {
        return jsonNode.get("schema");
      }

    } catch (Exception e) {
      LOGGER.debug("Failed to extract schema from event", e);
    }

    return null;
  }

  public static Map<String, String> getConfigSubset(Config config, String prefix) {
    final Map<String, String> ret = new HashMap<>();

    for (String propName : config.getPropertyNames()) {
      if (propName.startsWith(prefix)) {
        final String newPropName = propName.substring(prefix.length());
        ret.put(newPropName, config.getValue(propName, String.class));
      }
    }

    return ret;
  }

  public static Clustering getBigQueryClustering(String keyJsonString) {
    return BatchUtil.getBigQueryClustering(BatchUtil.getSchemaNode(keyJsonString));
  }

  public static Clustering getBigQueryClustering(JsonNode keySchema) {

    if (keySchema == null) {
      return Clustering.newBuilder().setFields(List.of("__source_ts_ms")).build();
    }

    ArrayList<String> fl = new ArrayList<>();
    for (JsonNode jsonSchemaFieldNode : keySchema.get("fields")) {
      String fieldName = jsonSchemaFieldNode.get("field").textValue();
      fl.add(fieldName);
    }

    fl.add("__source_ts_ms");
    return Clustering.newBuilder().setFields(fl).build();
  }

  public static Schema getBigQuerySchema(String eventJsonString, Boolean castDeletedField) {
    ArrayList<Field> fields = BatchUtil.getBigQuerySchemaFields(BatchUtil.getSchemaNode(eventJsonString), castDeletedField);

    if (fields == null) {
      return null;
    }

    fields.add(Field.of("__source_ts", StandardSQLTypeName.TIMESTAMP));
    return Schema.of(fields);
  }

  public static ArrayList<Field> getBigQuerySchemaFields(JsonNode eventSchema, Boolean castDeletedField) {

    if (eventSchema == null) {
      return null;
    }

    ArrayList<Field> fields = new ArrayList();

    String schemaType = eventSchema.get("type").textValue();
    String schemaName = "root";
    if (eventSchema.has("field")) {
      schemaName = eventSchema.get("field").textValue();
    }
    LOGGER.trace("Converting Schema of: {}::{}", schemaName, schemaType);

    for (JsonNode jsonSchemaFieldNode : eventSchema.get("fields")) {
      String fieldName = jsonSchemaFieldNode.get("field").textValue();
      String fieldType = jsonSchemaFieldNode.get("type").textValue();
      LOGGER.trace("Processing Field: {}.{}::{}", schemaName, fieldName, fieldType);
      // for all the debezium data types please see org.apache.kafka.connect.data.Schema;
      switch (fieldType) {
        case "int8":
        case "int16":
        case "int32":
        case "int64":
          fields.add(Field.of(fieldName, StandardSQLTypeName.INT64));
          break;
        case "float8":
        case "float16":
        case "float32":
        case "float64":
          fields.add(Field.of(fieldName, StandardSQLTypeName.FLOAT64));
          break;
        case "boolean":
          fields.add(Field.of(fieldName, StandardSQLTypeName.BOOL));
          break;
        case "string":
          fields.add((castDeletedField && Objects.equals(fieldName, "__deleted"))
              ? Field.of(fieldName, StandardSQLTypeName.BOOL)
              : Field.of(fieldName, StandardSQLTypeName.STRING));
          break;
        case "bytes":
          fields.add(Field.of(fieldName, StandardSQLTypeName.BYTES));
          break;
        case "array":
          fields.add(Field.of(fieldName, StandardSQLTypeName.ARRAY));
          break;
        case "map":
          fields.add(Field.of(fieldName, StandardSQLTypeName.STRUCT));
          break;
        case "struct":
          // recursive call
          ArrayList<Field> subFields = BatchUtil.getBigQuerySchemaFields(jsonSchemaFieldNode, false);
          fields.add(Field.newBuilder(fieldName, StandardSQLTypeName.STRUCT, FieldList.of(subFields)).build());
          break;
        default:
          // default to String type
          fields.add(Field.of(fieldName, StandardSQLTypeName.STRING));
          break;
      }
    }

    return fields;
  }


}
