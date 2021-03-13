/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import java.util.HashMap;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

public class TestDebeziumFilterProfile implements QuarkusTestProfile {

  // This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();
    config.put("debezium.sink.type", "batch");
    // config.put("quarkus.log.level", "DEBUG");
    config.put("debezium.sink.batch.cache.purge-on-startup", "true");

    // debezium unwrap message
    config.put("debezium.transforms", "unwrap,filter");
    config.put("debezium.transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
    config.put("debezium.transforms.unwrap.add.fields", "op,table,lsn,source.ts_ms");
    config.put("debezium.transforms.unwrap.add.headers", "db");
    config.put("debezium.transforms.unwrap.delete.handling.mode", "rewrite");
    // config.put("debezium.transforms", "filter");
    config.put("debezium.transforms.filter.type", "io.debezium.transforms.Filter");
    config.put("debezium.transforms.filter.language", "jsr223.groovy");
    config.put(
        "debezium.transforms.filter.condition",
        ""
            + "topic.endsWith('customers') && !("
            + "value.id == 1001 && "
            + "value.first_name == 'George__UPDATE2' && "
            + "value.__op == 'u')");
    return config;
  }
}
