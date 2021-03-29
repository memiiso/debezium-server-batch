/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.s3;

import io.debezium.server.batch.ConfigSource;
import io.debezium.server.batch.common.S3Minio;
import io.debezium.server.batch.common.SourcePostgresqlDB;
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
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(S3Minio.class)
@QuarkusTestResource(SourcePostgresqlDB.class)
@TestProfile(S3ChangeConsumerTestProfile.class)
public class S3ChangeConsumerTest {

  private static final int MESSAGE_COUNT = 4;

  static {
    Testing.Files.delete(ConfigSource.OFFSET_STORE_PATH);
    Testing.Files.createTestingFile(ConfigSource.OFFSET_STORE_PATH);
  }


  @ConfigProperty(name = "debezium.sink.type")
  String sinkType;


  @Test
  @Disabled
  public void testS3() {
    Testing.Print.enable();
    Assertions.assertThat(sinkType.equals("s3"));
    Awaitility.await().atMost(Duration.ofSeconds(60)).until(() ->
        S3Minio.getObjectList(ConfigSource.S3_BUCKET).size() >= MESSAGE_COUNT);
  }
}
