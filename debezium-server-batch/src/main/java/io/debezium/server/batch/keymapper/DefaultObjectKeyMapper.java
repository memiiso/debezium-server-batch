/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.batch.keymapper;

import io.debezium.engine.format.Json;

import java.time.LocalDateTime;

import org.eclipse.microprofile.config.ConfigProvider;

public class DefaultObjectKeyMapper implements ObjectKeyMapper {
  final String objectKeyPrefix = ConfigProvider.getConfig().getValue("debezium.sink.batch.objectkey.prefix", String.class);
  final String valueFormat = ConfigProvider.getConfig().getOptionalValue("debezium.format.value", String.class).orElse(Json.class.getSimpleName().toLowerCase());

  @Override
  public String map(String destination, LocalDateTime batchTime, String recordId) {
    String fname = batchTime.toString() + "-" + recordId + "." + valueFormat;
    return objectKeyPrefix + destination + "/" + fname;
  }

  @Override
  public String map(String destination, LocalDateTime batchTime, Integer batchId) {
    return this.map(destination, batchTime, batchId, valueFormat);
  }

  @Override
  public String map(String destination, LocalDateTime batchTime, Integer batchId, String fileExtension) {
    String fname = batchTime.toString() + "-" + batchId + "." + fileExtension;
    return objectKeyPrefix + destination + "/" + fname;
  }
}
