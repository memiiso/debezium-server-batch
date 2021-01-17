/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.server.DebeziumServer;
import io.debezium.server.testresource.TestDatabase;
import io.debezium.server.testresource.TestS3Minio;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

import java.time.Duration;
import javax.inject.Inject;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(TestS3Minio.class)
@QuarkusTestResource(TestDatabase.class)
public class SparkWriterIT extends BaseSparkIT {

  @Inject
  DebeziumServer server;
  @ConfigProperty(name = "debezium.sink.type")
  String sinkType;

  @ConfigProperty(name = "debezium.sink.batch.objectkey.mapper", defaultValue = "default")
  String customKeyMapper;

  @ConfigProperty(name = "debezium.sink.batch.objectkey-prefix", defaultValue = "")
  String objectKeyPrefix;
  @ConfigProperty(name = "debezium.sink.batch.s3.bucket-name", defaultValue = "")
  String bucket;

  @Test
  public void testS3Batch() {
    Testing.Print.enable();
    Assertions.assertThat(sinkType.equals("batch"));

    Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
      // return TestS3Minio.getIcebergDataFiles(ConfigSource.S3_BUCKET).size()
      try {
        Dataset<Row> df = spark.read().option("mergeSchema", "true")
            .parquet(bucket + "/debezium-cdc-testc.inventory.customers/*")
            .withColumn("input_file", functions.input_file_name());
        df.show(false);
        return df.filter("id is not null").count() >= 4;
      } catch (Exception e) {
        return false;
      }
    });
    TestS3Minio.listFiles();
  }

}
