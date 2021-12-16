/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.spark;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class BatchSparkBigqueryChangeConsumerTestProfile implements QuarkusTestProfile {

  //This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    config.put("debezium.sink.type", "sparkbigquerybatch");
    config.put("debezium.source.table.include.list", "inventory.customers");
    //
    config.put("debezium.sink.sparkbatch.spark.datasource.bigquery.project", "test");
    config.put("debezium.sink.sparkbatch.spark.datasource.bigquery.dataset", "test");
    config.put("debezium.sink.sparkbatch.spark.datasource.bigquery.temporaryGcsBucket", "bigquery-buffer-bucket");
    config.put("debezium.sink.sparkbatch.spark.datasource.bigquery.credentialsFile", "/path/to/application_credentials.json");
    // gcs token provider
    config.put("debezium.sink.sparkbatch.spark.hadoop.fs.gs.auth.access.token.provider.impl", "io.debezium.server.batch.spark.GCSAccessTokenProvider");
    config.put("debezium.sink.sparkbatch.spark.hadoop.credentialsFile", "/path/to/application_credentials.json");
    //
    config.put("quarkus.log.category.\"io.debezium.server.batch\".level", "INFO");
    config.put("quarkus.log.category.\"io.debezium.server.batch.spark.BatchSparkBigqueryChangeConsumer\".level", "DEBUG");
    config.put("quarkus.log.category.\"com.google.cloud.spark.bigquery\".level", "DEBUG");

    return config;
  }
}