/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.batch;

import java.net.URISyntaxException;
import java.time.Duration;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import io.debezium.server.testutils.TestS3Minio;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.server.DebeziumServer;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(TestS3Minio.class)
@QuarkusTestResource(io.debezium.server.batch.TestDatabase.class)
public class SparkIcebergBatchRecordWriterIT {

    @Inject
    DebeziumServer server;
    @ConfigProperty(name = "debezium.sink.type")
    String sinkType;

    {
        // Testing.Debug.enable();
        Testing.Files.delete(ConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(ConfigSource.OFFSET_STORE_PATH);
    }

    void setupDependencies(@Observes ConnectorStartedEvent event) throws URISyntaxException {
        if (!sinkType.equals("batch")) {
            return;
        }
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testS3Batch() throws Exception {
        Testing.Print.enable();
        Assertions.assertThat(sinkType.equals("batch"));

        Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
            // Testing.printError(s3server.getObjectList(ConfigSource.S3_BUCKET));
            // s3server.listFiles();
            return TestS3Minio.getIcebergDataFiles(ConfigSource.S3_BUCKET).size() >= 2;
        });
        //
        // SparkIcebergBatchRecordWriter bw = new SparkIcebergBatchRecordWriter(new LakeTableObjectKeyMapper());
        //
        // ObjectMapper objectMapper = new ObjectMapper();
        // bw.append("table1", objectMapper.readTree("{\"row1\": \"data\"}"));
        // bw.append("table1", objectMapper.readTree("{\"row1\": \"data\"}"));
        // bw.append("table1", objectMapper.readTree("{\"row1\": \"data\"}"));
        // bw.append("table1", objectMapper.readTree("{\"row1\": \"data\"}"));
        // bw.append("table1", objectMapper.readTree("{\"row1\": \"data\"}"));
        // bw.close();
        //
        // Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
        // List<Item> objects = s3server.getObjectList(ConfigSource.S3_BUCKET);
        // // we expect to see 2 batch files {0,1}
        // for (Item o : objects) {
        // if (o.toString().contains("table1") && o.toString().contains("-1.parquet")) {
        // Testing.print(objects.toString());
        // return true;
        // }
        // }
        // return false;
        // });
        //
        TestS3Minio.listFiles();
    }

}
