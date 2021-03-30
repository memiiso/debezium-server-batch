/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.cache.infinispan.cachetypes;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class InfinispanCacheSimpleTestProfile implements QuarkusTestProfile {

  //This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    config.put("debezium.sink.type", "batch");
    config.put("debezium.sink.batch.row-limit", "100");
    config.put("debezium.sink.batch.time-limit", "3000");
    config.put("debezium.source.max.batch.size", "500");
    config.put("debezium.source.poll.interval.ms", "10");
// ==================== SINK = CACHE ====================
    config.put("debezium.sink.batch.cache.memory-maxcount", "1254");
    config.put("debezium.sink.batch.cache.store", "simple");
    config.put("debezium.sink.batch.cache.purge-on-startup", "true");
    config.put("debezium.sink.batch.cache.max-batch-size", "1254");

    return config;
  }
}
