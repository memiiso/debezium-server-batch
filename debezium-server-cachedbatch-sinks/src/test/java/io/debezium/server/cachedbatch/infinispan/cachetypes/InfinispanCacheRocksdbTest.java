/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.cachedbatch.infinispan.cachetypes;

import io.debezium.server.cachedbatch.common.BaseSparkTest;
import io.debezium.server.cachedbatch.common.S3Minio;
import io.debezium.server.cachedbatch.common.SourcePostgresqlDB;
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
@TestProfile(InfinispanCacheRocksdbTestProfile.class)
public class InfinispanCacheRocksdbTest extends BaseSparkTest {

  @ConfigProperty(name = "debezium.sink.batch.row-limit")
  Integer maxBatchSize;

  @Test
  @Disabled // @TODO fix
  public void testSimpleUpload() throws Exception {

    PGCreateTestDataTable();
    PGLoadTestDataTable(maxBatchSize * 2);

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        Dataset<Row> df = getTableData("testc.inventory.test_date_table");
        return df.count() >= maxBatchSize * 2;
      } catch (Exception e) {
        return false;
      }
    });

  }

}
