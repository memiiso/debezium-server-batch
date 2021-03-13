/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.cachetypes;

import org.junit.jupiter.api.Test;

import io.debezium.server.batch.BatchTestConfigSource;
import io.debezium.server.batch.common.BaseSparkTest;
import io.debezium.server.batch.common.TestDatabase;
import io.debezium.server.batch.common.TestS3Minio;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3
 * destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(TestS3Minio.class)
@QuarkusTestResource(TestDatabase.class)
@TestProfile(TestCacheMemoryProfile.class)
public class TestCacheMemory extends BaseSparkTest {

  static {
    Testing.Files.delete(BatchTestConfigSource.OFFSET_STORE_PATH);
    Testing.Files.createTestingFile(BatchTestConfigSource.OFFSET_STORE_PATH);
  }

  @Test
  public void testPerformance() throws Exception {
    createDummyPerformanceTable();
    loadDataToDummyPerformanceTable(100000);
  }
}
