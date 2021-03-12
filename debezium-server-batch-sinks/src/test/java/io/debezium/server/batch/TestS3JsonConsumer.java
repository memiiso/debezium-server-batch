/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.server.batch.common.BaseSparkTest;
import io.debezium.server.batch.common.TestDatabase;
import io.debezium.server.batch.common.TestS3Minio;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;

import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(TestS3Minio.class)
@QuarkusTestResource(TestDatabase.class)
@TestProfile(TestS3JsonConsumerTestResource.class)
public class TestS3JsonConsumer extends BaseSparkTest {

  static {
    // Testing.Debug.enable();
    Testing.Files.delete(ConfigSource.OFFSET_STORE_PATH);
    Testing.Files.createTestingFile(ConfigSource.OFFSET_STORE_PATH);
  }


  @ConfigProperty(name = "debezium.sink.type")
  String sinkType;

  @Test
  @Disabled
  public void simpleUploadTest() {
    Testing.Print.enable();
    Assertions.assertThat(sinkType.equals("batch"));

    Awaitility.await().atMost(Duration.ofSeconds(ConfigSource.waitForSeconds())).until(() -> {
      TestS3Minio.listFiles();
      return TestS3Minio.getIcebergDataFiles(ConfigSource.S3_BUCKET).size() > 4;
    });

    TestS3Minio.listFiles();
  }
}
