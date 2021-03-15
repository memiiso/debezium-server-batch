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
public class BatchJsonlinesFile {
  private final File file;
  private JsonNode schema;

  public BatchJsonlinesFile(File file, JsonNode schema) {
    this.file = file;
    this.schema = schema;
  }

  public File getFile() {
    return file;
  }

  public JsonNode getSchema() {
    return schema;
  }

  public void setSchema(JsonNode schema) {
    this.schema = schema;
  }
}



