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

import static io.debezium.server.batch.ConfigSource.S3_BUCKET;

public class SparkIcebergConsumerTestProfile implements QuarkusTestProfile {

  //This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    config.put("debezium.sink.type", "batch");
    config.put("quarkus.arc.selected-alternatives", "SparkIcebergWriter");
    config.put("debezium.sink.sparkbatch.spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    config.put("debezium.sink.sparkbatch.spark.sql.catalog.spark_catalog.type", "hadoop");
    config.put("debezium.sink.sparkbatch.spark.sql.catalog.spark_catalog.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog");
    config.put("debezium.sink.sparkbatch.spark.sql.catalog.spark_catalog.warehouse", "s3a://" + S3_BUCKET + "/iceberg_warehouse");
    //s3Test.put("debezium.sink.sparkbatch.spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    //s3Test.put("debezium.sink.sparkbatch.spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    //s3Test.put("debezium.sink.sparkbatch.spark.sql.catalog.spark_catalog.type", "hadoop");
    //s3Test.put("debezium.sink.sparkbatch.spark.sql.catalog.spark_catalog.warehouse", "s3a://" + S3_BUCKET + "/iceberg_warehouse");
    //s3Test.put("debezium.sink.sparkbatch.spark.sql.warehouse.dir", "s3a://" + S3_BUCKET + "/iceberg_warehouse");
    //s3Test.put("debezium.sink.sparkbatch.spark.delta.logStore.class", "org.apache.spark.sql.delta.storage" +  ".S3SingleDriverLogStore");

    config.put("debezium.sink.icebergsparkbatch.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog");
    config.put("debezium.sink.icebergsparkbatch.warehouse", "s3a://" + S3_BUCKET + "/iceberg_warehouse");

    return config;
  }
}
