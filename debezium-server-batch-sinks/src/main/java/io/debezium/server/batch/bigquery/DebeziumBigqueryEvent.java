/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.bigquery;

import io.debezium.server.batch.DebeziumEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ismail Simsek
 */
public class DebeziumBigqueryEvent {
  protected static final Logger LOGGER = LoggerFactory.getLogger(DebeziumBigqueryEvent.class);

  private static ImmutableMap<Field.Mode, TableFieldSchema.Mode> BQTableSchemaModeMap =
      ImmutableMap.of(
          Field.Mode.NULLABLE, TableFieldSchema.Mode.NULLABLE,
          Field.Mode.REPEATED, TableFieldSchema.Mode.REPEATED,
          Field.Mode.REQUIRED, TableFieldSchema.Mode.REQUIRED);

  private static ImmutableMap<StandardSQLTypeName, TableFieldSchema.Type> BQTableSchemaTypeMap =
      new ImmutableMap.Builder<StandardSQLTypeName, TableFieldSchema.Type>()
          .put(StandardSQLTypeName.BOOL, TableFieldSchema.Type.BOOL)
          .put(StandardSQLTypeName.BYTES, TableFieldSchema.Type.BYTES)
          .put(StandardSQLTypeName.DATE, TableFieldSchema.Type.DATE)
          .put(StandardSQLTypeName.DATETIME, TableFieldSchema.Type.DATETIME)
          .put(StandardSQLTypeName.FLOAT64, TableFieldSchema.Type.DOUBLE)
          .put(StandardSQLTypeName.GEOGRAPHY, TableFieldSchema.Type.GEOGRAPHY)
          .put(StandardSQLTypeName.INT64, TableFieldSchema.Type.INT64)
          .put(StandardSQLTypeName.NUMERIC, TableFieldSchema.Type.NUMERIC)
          .put(StandardSQLTypeName.STRING, TableFieldSchema.Type.STRING)
          .put(StandardSQLTypeName.STRUCT, TableFieldSchema.Type.STRUCT)
          .put(StandardSQLTypeName.TIME, TableFieldSchema.Type.TIME)
          .put(StandardSQLTypeName.TIMESTAMP, TableFieldSchema.Type.TIMESTAMP)
          .build();

  final DebeziumEvent event;

  public DebeziumBigqueryEvent(DebeziumEvent event) {
    this.event = event;
  }

  public Clustering getBigQueryClustering() {
    // special destinations like "heartbeat.topics"
    if (event.destination().startsWith("__debezium")) {
      return Clustering.newBuilder().setFields(List.of("__source_ts_ms")).build();
    } else if (event.keySchema() == null) {
      return Clustering.newBuilder().setFields(List.of("__source_ts_ms")).build();
    } else {
      return getBigQueryClustering(event.keySchema());
    }
  }

  private static ArrayList<Field> getBigQuerySchemaFields(JsonNode schemaNode, Boolean castDeletedField, Boolean binaryAsString) {

    if (schemaNode == null) {
      return null;
    }

    ArrayList<Field> fields = new ArrayList<>();

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
          if (binaryAsString) {
            fields.add(Field.of(fieldName, StandardSQLTypeName.STRING));
          } else {
            fields.add(Field.of(fieldName, StandardSQLTypeName.BYTES));
          }
          break;
        case "array":
          fields.add(Field.of(fieldName, StandardSQLTypeName.ARRAY));
          break;
        case "map":
          fields.add(Field.of(fieldName, StandardSQLTypeName.STRUCT));
          break;
        case "struct":
          // recursive call
          ArrayList<Field> subFields = getBigQuerySchemaFields(jsonSchemaFieldNode, false, binaryAsString);
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

  public String getBigQueryClusteringFields() {

    if (event.keySchema() == null) {
      return "__source_ts";
    }

    List<String> keyFields = getBigQuerySchemaFields(event.keySchema(), false, false)
        .stream()
        .map(Field::getName)
        .collect(Collectors.toList());

    if (keyFields.isEmpty()) {
      return "__source_ts";
    }

    return StringUtils.strip(String.join(",", keyFields) + ",__source_ts", ",");
  }

  public Schema getBigQuerySchema(Boolean castDeletedField) {
    return getBigQuerySchema(castDeletedField, false);
  }

  private static Clustering getBigQueryClustering(JsonNode schemaNode) {

    ArrayList<String> clusteringFields = new ArrayList<>();
    for (JsonNode jsonSchemaFieldNode : schemaNode.get("fields")) {
      // NOTE Limit clustering fields to 4. it's the limit of Bigquery 
      if (clusteringFields.size() >= 3) {
        break;
      }

      String fieldName = jsonSchemaFieldNode.get("field").textValue();
      clusteringFields.add(fieldName);
    }

    clusteringFields.add("__source_ts_ms");
    return Clustering.newBuilder().setFields(clusteringFields).build();
  }

  public Schema getBigQuerySchema(Boolean castDeletedField, Boolean binaryAsString) {
    ArrayList<Field> fields = getBigQuerySchemaFields(event.valueSchema(), castDeletedField, binaryAsString);

    if (fields == null) {
      return null;
    }

    // partitioning field added by Bigquery consumer
    fields.add(Field.of("__source_ts", StandardSQLTypeName.TIMESTAMP));
    // special destinations like "heartbeat.topics" might not have __source_ts_ms field. 
    // which is used to cluster BQ tables
    if (!fields.contains(Field.of("__source_ts_ms", StandardSQLTypeName.INT64))) {
      fields.add(Field.of("__source_ts_ms", StandardSQLTypeName.INT64));
    }
    return Schema.of(fields);
  }

  public static TableSchema convertBigQuerySchema2TableSchema(Schema schema) {

    TableSchema.Builder result = TableSchema.newBuilder();
    for (int i = 0; i < schema.getFields().size(); i++) {
      result.addFields(i, convertFieldSchema(schema.getFields().get(i)));
    }
    return result.build();
  }

  public TableSchema getBigQueryTableSchema(Boolean castDeletedField) {
    Schema schema = getBigQuerySchema(castDeletedField);
    return DebeziumBigqueryEvent.convertBigQuerySchema2TableSchema(schema);
  }

  /**
   * Converts from bigquery v2 Field Schema to bigquery storage API Field Schema.
   *
   * @param field the BigQuery client Field Schema
   * @return the bigquery storage API Field Schema
   */
  private static TableFieldSchema convertFieldSchema(Field field) {
    TableFieldSchema.Builder result = TableFieldSchema.newBuilder();
    if (field.getMode() == null) {
      field = field.toBuilder().setMode(Field.Mode.NULLABLE).build();
    }
    result.setMode(BQTableSchemaModeMap.get(field.getMode()));
    result.setName(field.getName());
    result.setType(BQTableSchemaTypeMap.get(field.getType().getStandardType()));
    if (field.getDescription() != null) {
      result.setDescription(field.getDescription());
    }
    if (field.getSubFields() != null) {
      for (int i = 0; i < field.getSubFields().size(); i++) {
        result.addFields(i, convertFieldSchema(field.getSubFields().get(i)));
      }
    }
    return result.build();
  }

}
