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

import static io.debezium.server.batch.common.SourceMysqlDB.*;

public class TestMysqlProfile implements QuarkusTestProfile {

  //This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    config.put("debezium.source.connector.class", "io.debezium.connector.mysql.MySqlConnector");
    config.put("debezium.sink.type", "batch");
    config.put("debezium.source.max.batch.size", "70000");
    config.put("debezium.source.max.queue.size", "700000");
    // 30000 30-second
    config.put("debezium.source.poll.interval.ms", "10000");
    config.put("quarkus.log.level", "INFO");
    config.put("debezium.source.internal.implementation", "legacy");
    config.put("debezium.source.database.hostname", MYSQL_HOST);
    config.put("debezium.source.database.port", MYSQL_PORT_DEFAULT.toString());
    config.put("debezium.source.database.user", MYSQL_DEBEZIUM_USER);
    config.put("debezium.source.database.password", MYSQL_DEBEZIUM_PASSWORD);
    config.put("debezium.source.database.dbname", MYSQL_DATABASE);
    config.put("debezium.source.database.include.list", MYSQL_DATABASE);

    return config;
  }
}
