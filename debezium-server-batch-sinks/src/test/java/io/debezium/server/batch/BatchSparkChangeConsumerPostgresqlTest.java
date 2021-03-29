/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.server.batch.common.BaseSparkTest;
import io.debezium.server.batch.common.S3Minio;
import io.debezium.server.batch.common.SourcePostgresqlDB;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(S3Minio.class)
@QuarkusTestResource(SourcePostgresqlDB.class)
@TestProfile(BatchSparkChangeConsumerPostgresqlTestProfile.class)
public class BatchSparkChangeConsumerPostgresqlTest extends BaseSparkTest {


  @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = "1000")
  Integer maxBatchSize;

  static {
    Testing.Files.delete(ConfigSource.OFFSET_STORE_PATH);
    Testing.Files.createTestingFile(ConfigSource.OFFSET_STORE_PATH);
  }

  @Test
  @Disabled
  public void testPerformance() throws Exception {

    int iteration = 10;
    createPGDummyPerformanceTable();

    new Thread(() -> {
      try {
        for (int i = 0; i <= iteration; i++) {
          loadPGDataToDummyPerformanceTable(maxBatchSize);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }).start();

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.dummy_performance_table");
        return df.count() >= (long) iteration * maxBatchSize;
      } catch (Exception e) {
        return false;
      }
    });
  }

}
