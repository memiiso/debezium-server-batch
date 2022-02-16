/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.bigquery.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ismail Simsek
 */
public class BatchEvent {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BatchEvent.class);
  protected final String destination;
  protected final JsonNode value;
  protected final JsonNode key;
  protected final JsonNode valueSchema;
  protected final JsonNode keySchema;

  public BatchEvent(String destination,
                    JsonNode value,
                    JsonNode key,
                    JsonNode valueSchema,
                    JsonNode keySchema) {
    this.destination = destination;
    this.value = value;
    this.key = key;
    this.valueSchema = valueSchema;
    this.keySchema = keySchema;
  }

  private static StructType getSparkDfSchema(JsonNode schemaNode) {

    if (schemaNode == null) {
      return null;
    }

    StructType sparkSchema = new StructType();

    String schemaType = schemaNode.get("type").textValue();
    String schemaName = "root";
    if (schemaNode.has("field")) {
      schemaName = schemaNode.get("field").textValue();
    }
    LOGGER.trace("Converting Schema of: {}::{}", schemaName, schemaType);

    for (JsonNode jsonSchemaFieldNode : schemaNode.get("fields")) {
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
          StructType subSchema = getSparkDfSchema(jsonSchemaFieldNode);
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

  private static Clustering getBigQueryClustering(JsonNode schemaNode) {

    if (schemaNode == null) {
      return Clustering.newBuilder().setFields(List.of("__source_ts_ms")).build();
    }

    ArrayList<String> clusteringFields = new ArrayList<>();
    for (JsonNode jsonSchemaFieldNode : schemaNode.get("fields")) {
      // NOTE Limit clustering fields to 4. it's the limit of Bigquery 
      if (clusteringFields.size() <= 3) {
        String fieldName = jsonSchemaFieldNode.get("field").textValue();
        clusteringFields.add(fieldName);
      }
    }

    clusteringFields.add("__source_ts_ms");
    return Clustering.newBuilder().setFields(clusteringFields).build();
  }

  private static ArrayList<Field> getBigQuerySchemaFields(JsonNode schemaNode, Boolean castDeletedField) {

    if (schemaNode == null) {
      return null;
    }

    ArrayList<Field> fields = new ArrayList();

    String schemaType = schemaNode.get("type").textValue();
    String schemaName = "root";
    if (schemaNode.has("field")) {
      schemaName = schemaNode.get("field").textValue();
    }
    LOGGER.trace("Converting Schema of: {}::{}", schemaName, schemaType);

    for (JsonNode jsonSchemaFieldNode : schemaNode.get("fields")) {
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
          ArrayList<Field> subFields = getBigQuerySchemaFields(jsonSchemaFieldNode, false);
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

  public String destination() {
    return destination;
  }

  public JsonNode value() {
    return value;
  }

  public JsonNode key() {
    return key;
  }

  public StructType getSparkDfSchema() {
    return getSparkDfSchema(valueSchema);
  }

  public Clustering getBigQueryClustering() {
    return getBigQueryClustering(keySchema);
  }

  public String getBigQueryClusteringFields() {

    if (keySchema == null) {
      return "__source_ts";
    }

    List<String> keyFields = getBigQuerySchemaFields(keySchema, false)
        .stream()
        .map(Field::getName)
        .collect(Collectors.toList());

    if (keyFields.isEmpty()) {
      return "__source_ts";
    }

    return StringUtils.strip(String.join(",", keyFields) + ",__source_ts", ",");
  }

  public Schema getBigQuerySchema(Boolean castDeletedField) {
    ArrayList<Field> fields = getBigQuerySchemaFields(valueSchema, castDeletedField);

    if (fields == null) {
      return null;
    }

    fields.add(Field.of("__source_ts", StandardSQLTypeName.TIMESTAMP));
    return Schema.of(fields);
  }


}
