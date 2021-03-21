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
import io.debezium.server.batch.common.SourceMysqlDB;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(S3Minio.class)
@QuarkusTestResource(SourceMysqlDB.class)
@TestProfile(TestMysqlProfile.class)
public class TestMysql extends BaseSparkTest {

  static {
    Testing.Files.delete(ConfigSource.OFFSET_STORE_PATH);
    Testing.Files.createTestingFile(ConfigSource.OFFSET_STORE_PATH);
  }

  @Test
  public void testPerformance() throws Exception {

    int batch = 15;
    int iteration = 100;
    int rowsCreated = iteration * batch;

    createMysqlDummyPerformanceTable();
    new Thread(() -> {
      try {
        for (int i = 0; i <= iteration; i++) {
          //Thread.sleep(10000);
          loadMysqlDataToDummyPerformanceTable(batch);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }).start();

    Awaitility.await().atMost(Duration.ofSeconds(80000)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.dummy_performance_table");
        return df.count() >= rowsCreated;
      } catch (Exception e) {
        return false;
      }
    });
  }


}
