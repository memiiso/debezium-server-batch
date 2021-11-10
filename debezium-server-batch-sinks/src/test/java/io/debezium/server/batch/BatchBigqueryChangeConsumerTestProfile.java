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

public class BatchBigqueryChangeConsumerTestProfile implements QuarkusTestProfile {

  //This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    config.put("debezium.sink.type", "bigquerybatch");
    config.put("debezium.source.table.include.list", "inventory.customers,inventory.geom");
    config.put("debezium.sink.bigquerybatch.cast-deleted-field", "true");
    //
    config.put("debezium.sink.bigquerybatch.project", "test");
    config.put("debezium.sink.bigquerybatch.dataset", "test");
    config.put("debezium.sink.bigquerybatch.credentialsFile", "/path/to/application_credentials.json");
    // logging
    config.put("quarkus.log.category.\"io.debezium.server.batch\".level", "INFO");
    config.put("quarkus.log.category.\"io.debezium.server.batch.BatchBigqueryChangeConsumer\".level", "INFO");
    config.put("quarkus.log.category.\"com.google.cloud.bigquery\".level", "INFO");
    return config;
  }
}
