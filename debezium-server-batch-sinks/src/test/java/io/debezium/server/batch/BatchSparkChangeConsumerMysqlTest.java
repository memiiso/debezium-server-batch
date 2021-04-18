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
@QuarkusTestResource(SourceMysqlDB.class)
@TestProfile(BatchSparkChangeConsumerMysqlTestProfile.class)
@Disabled // @TODO fix
public class BatchSparkChangeConsumerMysqlTest extends BaseSparkTest {


  @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = "1000")
  Integer maxBatchSize;

  @Test
  @Disabled // @TODO fix
  public void testPerformance() throws Exception {

    int iteration = 10;
    mysqlCreateTestDataTable();
    for (int i = 0; i <= iteration; i++) {
      mysqlLoadTestDataTable(maxBatchSize);
    }

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.test_date_table");
        return df.count() >= (long) iteration * maxBatchSize;
      } catch (Exception e) {
        return false;
      }
    });
  }


}
