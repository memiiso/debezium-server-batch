/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.cachedbatch;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class MemoryCacheTestProfile implements QuarkusTestProfile {

  //This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    config.put("quarkus.arc.selected-alternatives", "MemoryCache");
    config.put("debezium.sink.batch.row-limit", "20000");
    config.put("debezium.sink.batch.time-limit", "3000");
    config.put("debezium.source.max.batch.size", "20000");
    config.put("debezium.source.max.queue.size", "40000");
    config.put("debezium.source.poll.interval.ms", "20000");

    return config;
  }
}
