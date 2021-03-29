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

public class BatchSparkChangeConsumerPostgresqlTestProfile implements QuarkusTestProfile {

  //This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    config.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
    config.put("debezium.sink.type", "batch");
    config.put("debezium.source.max.batch.size", "7000");
    config.put("debezium.source.max.queue.size", "70000");
    // 30000 30-second
    config.put("debezium.source.poll.interval.ms", "60000");

    return config;
  }
}
