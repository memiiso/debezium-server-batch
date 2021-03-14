/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.infinispan.cachetypes;

import io.debezium.server.batch.ConfigSource;
import io.debezium.server.batch.common.BaseSparkTest;
import io.debezium.server.batch.common.TestDatabase;
import io.debezium.server.batch.common.TestS3Minio;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3
 * destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(TestS3Minio.class)
@QuarkusTestResource(TestDatabase.class)
@TestProfile(TestCacheSimpleProfile.class)
public class TestCacheSimple extends BaseSparkTest {

  static {
    Testing.Files.delete(ConfigSource.OFFSET_STORE_PATH);
    Testing.Files.createTestingFile(ConfigSource.OFFSET_STORE_PATH);
  }

  @Test
  public void testPerformance() throws Exception {
    createDummyPerformanceTable();
    loadDataToDummyPerformanceTable(100000);
  }
}
