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
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(TestS3Minio.class)
@QuarkusTestResource(TestDatabase.class)
public class S3IT {

  private static final int MESSAGE_COUNT = 4;
  protected static S3Client s3client = null;

  static {
    Testing.Files.delete(ConfigSource.OFFSET_STORE_PATH);
    Testing.Files.createTestingFile(ConfigSource.OFFSET_STORE_PATH);
  }

  @Inject
  DebeziumServer server;
  @ConfigProperty(name = "debezium.sink.type")
  String sinkType;

  void setupDependencies(@Observes ConnectorStartedEvent event) {
    if (!sinkType.equals("s3")) {
      return;
    }
  }

  void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
    if (!event.isSuccess()) {
      throw (Exception) event.getError().get();
    }
  }

  @Test
  public void testS3() {
    Testing.Print.enable();
    Assertions.assertThat(sinkType.equals("s3"));
    Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() ->
        TestS3Minio.getObjectList(ConfigSource.S3_BUCKET).size() >= MESSAGE_COUNT);
  }
}
