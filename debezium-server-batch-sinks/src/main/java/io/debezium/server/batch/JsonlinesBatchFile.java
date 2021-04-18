/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import java.io.File;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public class JsonlinesBatchFile {
  private final File file;
  private JsonNode valSchema;
  private JsonNode keySchema;

  public JsonlinesBatchFile(File file, JsonNode valSchema) {
    this(file, valSchema, null);
  }

  public JsonlinesBatchFile(File file, JsonNode valSchema, JsonNode keySchema) {
    this.file = file;
    this.valSchema = valSchema;
    this.keySchema = keySchema;
  }

  public File getFile() {
    return file;
  }

  public JsonNode getValSchema() {
    return valSchema;
  }

  public void setValSchema(JsonNode valSchema) {
    this.valSchema = valSchema;
  }

  public JsonNode getKeySchema() {
    return keySchema;
  }

  public void setKeySchema(JsonNode keySchema) {
    this.keySchema = keySchema;
  }

}

