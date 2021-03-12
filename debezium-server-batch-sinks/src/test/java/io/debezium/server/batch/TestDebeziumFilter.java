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
@QuarkusTestResource(TestS3Minio.class)
@QuarkusTestResource(TestDatabase.class)
@TestProfile(TestDebeziumFilterProfile.class)
public class TestDebeziumFilter extends BaseSparkTest {


  static {
    Testing.Files.delete(ConfigSource.OFFSET_STORE_PATH);
    Testing.Files.createTestingFile(ConfigSource.OFFSET_STORE_PATH);
  }

  @Test
  public void testFiltering() throws Exception {
    TestDatabase.runSQL("UPDATE inventory.customers SET first_name='George__UPDATE1' WHERE ID = 1002 ;");
    TestDatabase.runSQL("UPDATE inventory.customers SET first_name='George__UPDATE2' WHERE ID = 1001 ;");
    TestDatabase.runSQL("INSERT INTO inventory.customers (id, first_name,last_name ,email) VALUES (default," +
        "'SallyUSer2', 'lastname','email@email.com')");

    Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
      try {
        Dataset<Row> ds = getTableData("testc.inventory.customers");
        ds.show(false);
        return ds.where("first_name == 'George__UPDATE2'").count() == 0
            && ds.where("first_name == 'George__UPDATE1'").count() == 1
            && ds.where("first_name == 'SallyUSer2'").count() == 1
            && ds.count() >= 5;
      } catch (Exception e) {
        return false;
      }
    });

  }

}
