/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.bigquery;

import io.debezium.server.batch.shared.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.io.IOException;
import java.time.Duration;
import javax.inject.Inject;

import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1.TableName;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import static io.debezium.server.batch.shared.BaseSparkTest.PGCreateTestDataTable;
import static io.debezium.server.batch.shared.BaseSparkTest.PGLoadTestDataTable;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(SourcePostgresqlDB.class)
@TestProfile(BatchBigqueryChangeConsumerTestProfile.class)
@Disabled("manual run")
public class BatchBigqueryChangeConsumerTest {

  @Inject
  BatchBigqueryChangeConsumer bqchangeConsumer;

  public TableResult simpleQuery(String query) throws InterruptedException {

    if (bqchangeConsumer.bqClient == null) {
      bqchangeConsumer.initizalize();
    }
    //System.out.println(query);
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
    return bqchangeConsumer.bqClient.query(queryConfig);
  }
  
  public TableResult dropTable(String destination) throws InterruptedException {
    TableId tableId = bqchangeConsumer.getTableId(destination);
    return this.simpleQuery("DROP TABLE IF EXISTS " + tableId.getProject() + "." + tableId.getDataset() + "." + tableId.getTable());
  }
  
  public TableResult getTableData(String destination) throws InterruptedException {
    TableId tableId = bqchangeConsumer.getTableId(destination);
    return this.simpleQuery("SELECT * FROM " + tableId.getProject() + "." + tableId.getDataset() + "." + tableId.getTable());
  }

  @Test
  public void testSimpleUpload() throws InterruptedException {
    dropTable("testc.inventory.geom");
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        TableResult result = this.getTableData("testc.inventory.geom");
        result.iterateAll().forEach(System.out::println);
        return result.getTotalRows() >= 3;
      } catch (Exception e) {
        return false;
      }
    });

    dropTable("testc.inventory.customers");
    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      try {
        TableResult result = this.getTableData("testc.inventory.customers");
        result.iterateAll().forEach(System.out::println);
        return result.getTotalRows() >= 4;
      } catch (Exception e) {
        return false;
      }
    });
  }

  @Test
  @Disabled
  public void testPerformance() throws Exception {
    this.testPerformance(1500);
  }

  public void testPerformance(int maxBatchSize) throws Exception {
    int iteration = 10;
    PGCreateTestDataTable();
    for (int i = 0; i <= iteration; i++) {
      new Thread(() -> {
        try {
          PGLoadTestDataTable(maxBatchSize, false);
        } catch (Exception e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }).start();
    }

    Awaitility.await().atMost(Duration.ofSeconds(1200)).until(() -> {
      try {
        TableResult result = this.getTableData("testc.inventory.test_date_table");
        return result.getTotalRows() >= (long) iteration * maxBatchSize;
      } catch (Exception e) {
        return false;
      }
    });

    TableResult result = this.getTableData("testc.inventory.test_date_table");
    System.out.println("Row Count=" + result.getTotalRows());
  }
}
