/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.server.batch.common.SourcePostgresqlDB;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;
import javax.inject.Inject;

import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
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
@QuarkusTestResource(SourcePostgresqlDB.class)
@TestProfile(BatchBigqueryChangeConsumerTestProfile.class)
@Disabled("manual run")
public class BatchBigqueryChangeConsumerTest {

  @ConfigProperty(name = "debezium.sink.type")
  String sinkType;

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

  public TableResult getTableData(String destination) throws InterruptedException {
    TableId tableId = bqchangeConsumer.getTableId(destination);
    return this.simpleQuery("SELECT * FROM " + tableId.getProject() + "." + tableId.getDataset() + "." + tableId.getTable());
  }

  @Test
  public void testSimpleUpload() {
    Testing.Print.enable();

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        TableResult result = this.getTableData("testc.inventory.geom");
        result.iterateAll().forEach(System.out::println);
        return result.getTotalRows() >= 3;
      } catch (Exception e) {
        return false;
      }
    });

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
}
