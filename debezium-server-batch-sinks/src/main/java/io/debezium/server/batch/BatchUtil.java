/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public class BatchUtil {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BatchUtil.class);
  protected static final ObjectMapper jsonObjectMapper = new ObjectMapper();

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

}
