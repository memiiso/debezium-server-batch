/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.uploadlock;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class LocalFileUploadLockTestProfile implements QuarkusTestProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();
    config.put("debezium.sink.batch.upload-lock", "LocalFileUploadLock");
    config.put("debezium.sink.batch.upload-lock.max-wait-ms", "5000");
    config.put("debezium.sink.batch.upload-lock.wait-interval-ms", "2000");
    config.put("io.debezium.server.batch.uploadlock", "DEBUG");
    return config;
  }
}