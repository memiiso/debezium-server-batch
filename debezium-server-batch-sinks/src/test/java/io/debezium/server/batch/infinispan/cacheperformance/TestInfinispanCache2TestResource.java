/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.infinispan.cacheperformance;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class TestInfinispanCache2TestResource implements QuarkusTestProfile {

  //This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    config.put("debezium.sink.batch.row-limit", "100000");
    config.put("debezium.sink.batch.cache.store", "simple");
    config.put("debezium.sink.batch.cache.purge-on-startup", "true");
    config.put("debezium.sink.batch.cache.jsonlines-writer-buffer-kb", "32");

    return config;
  }
}
