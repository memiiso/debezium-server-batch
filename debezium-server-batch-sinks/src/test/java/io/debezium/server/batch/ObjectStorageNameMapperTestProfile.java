/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class ObjectStorageNameMapperTestProfile implements QuarkusTestProfile {

  //This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    config.put("debezium.sink.batch.objectkey-partition", "true");
    config.put("debezium.sink.batch.objectkey-partition-time-zone", "UTC");
    config.put("debezium.sink.batch.objectkey-prefix", "myprefix");
    config.put("debezium.sink.batch.objectkey-regexp", "\\d");
    config.put("debezium.sink.batch.objectkey-regexp-replace", "");
    return config;
  }
}
