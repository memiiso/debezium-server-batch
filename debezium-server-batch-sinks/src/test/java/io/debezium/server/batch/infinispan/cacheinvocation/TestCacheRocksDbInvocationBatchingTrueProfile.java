/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.infinispan.cacheinvocation;

import java.util.HashMap;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

public class TestCacheRocksDbInvocationBatchingTrueProfile implements QuarkusTestProfile {

  // This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    config.put("debezium.sink.type", "batch");
    config.put("debezium.sink.batch.writer", "spark");
    config.put("debezium.sink.batch.row-limit", "100000");
    config.put("debezium.sink.batch.time-limit", "3000");
    config.put("debezium.source.max.batch.size", "254");
    config.put("debezium.source.poll.interval.ms", "10");
    // ==================== SINK = CACHE ====================
    config.put("debezium.sink.batch.cache.memory-maxcount", "254");
    config.put("debezium.sink.batch.cache-store", "rocksdb");
    config.put("debezium.sink.batch.cache.use-batch-append", "true");
    config.put("debezium.sink.batch.cache.purge-on-startup", "true");
    config.put("debezium.sink.batch.cache.invocation-batching", "true");
    config.put("debezium.sink.batch.cache.max-batch-size", "254");

    return config;
  }
}