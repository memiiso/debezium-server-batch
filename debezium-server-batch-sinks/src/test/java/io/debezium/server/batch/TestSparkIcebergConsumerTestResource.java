/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import static io.debezium.server.batch.BatchTestConfigSource.S3_BUCKET;

import java.util.HashMap;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

public class TestSparkIcebergConsumerTestResource implements QuarkusTestProfile {

  // This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    config.put("debezium.sink.type", "batch");
    config.put("debezium.sink.batch.writer", "sparkiceberg");
    config.put(
        "debezium.sink.sparkbatch.spark.sql.catalog.spark_catalog",
        "org.apache.iceberg.spark.SparkSessionCatalog");
    config.put("debezium.sink.sparkbatch.spark.sql.catalog.spark_catalog.type", "hadoop");
    config.put(
        "debezium.sink.sparkbatch.spark.sql.catalog.spark_catalog.catalog-impl",
        "org.apache.iceberg.hadoop.HadoopCatalog");
    config.put(
        "debezium.sink.sparkbatch.spark.sql.catalog.spark_catalog.warehouse",
        "s3a://" + S3_BUCKET + "/iceberg_warehouse");
    return config;
  }
}
