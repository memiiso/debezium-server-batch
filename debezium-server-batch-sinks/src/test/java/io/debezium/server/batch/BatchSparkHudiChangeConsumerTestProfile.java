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

public class BatchSparkHudiChangeConsumerTestProfile implements QuarkusTestProfile {

  //This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    config.put("debezium.sink.type", "sparkhudibatch");
    config.put("debezium.sink.sparkbatch.spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    config.put("quarkus.log.category.\"org.apache.hudi\".level", "WARN");
    config.put("debezium.source.max.batch.size", "10");

    return config;
  }
}
