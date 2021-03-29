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

public class BatchSparkChangeConsumerMysqlTestProfile implements QuarkusTestProfile {

  //This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    config.put("%mysql.debezium.source.connector.class", "io.debezium.connector.mysql.MySqlConnector");
    config.put("debezium.sink.type", "batch");
    config.put("debezium.source.max.batch.size", "70000");
    config.put("debezium.source.max.queue.size", "700000");
    // 30000 30-second
    config.put("debezium.source.poll.interval.ms", "10000");
    config.put("debezium.source.internal.implementation", "legacy");

    return config;
  }

  @Override
  public String getConfigProfile() {
    return "mysql";
  }

}
