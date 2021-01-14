/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.batch;

import static io.debezium.server.batch.ConfigSource.S3_BUCKET;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

import javax.inject.Inject;

import io.debezium.server.testresource.TestDatabase;
import io.debezium.server.testresource.TestS3Minio;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

import io.debezium.server.DebeziumServer;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(TestS3Minio.class)
@QuarkusTestResource(TestDatabase.class)
public class IcebergIT extends BaseSparkIT {

    @Inject
    DebeziumServer server;
    @ConfigProperty(name = "debezium.sink.type")
    String sinkType;

    @Test
    public void testIcebergConsumer() throws Exception {
        // Testing.Print.enable();
        assertEquals(sinkType, "iceberg");

        Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
            try {

                Dataset<Row> ds = spark.read().format("iceberg")
                        .load("s3a://test-bucket/iceberg_warehouse/testc.inventory.customers");
                ds.show();
                return ds.count() >= 4;
            }
            catch (Exception e) {
                return false;
            }
        });

        Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(
                () -> TestS3Minio.getIcebergDataFiles(S3_BUCKET).size() >= 2);
        TestS3Minio.listFiles();
    }
}
