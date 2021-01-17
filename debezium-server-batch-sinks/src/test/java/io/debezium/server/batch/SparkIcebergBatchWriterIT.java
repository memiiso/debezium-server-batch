/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.server.DebeziumServer;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.server.testresource.TestDatabase;
import io.debezium.server.testresource.TestS3Minio;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

import java.time.Duration;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

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
public class SparkIcebergBatchWriterIT {

  @Inject
  DebeziumServer server;
  @ConfigProperty(name = "debezium.sink.type")
  String sinkType;

  static {
    // Testing.Debug.enable();
    Testing.Files.delete(ConfigSource.OFFSET_STORE_PATH);
    Testing.Files.createTestingFile(ConfigSource.OFFSET_STORE_PATH);
  }

  void setupDependencies(@Observes ConnectorStartedEvent event) {
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
  public void testS3Batch() {
    Testing.Print.enable();
    Assertions.assertThat(sinkType.equals("batch"));

    Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
      return TestS3Minio.getIcebergDataFiles(ConfigSource.S3_BUCKET).size() >= 2;
    });
    TestS3Minio.listFiles();
  }

}
