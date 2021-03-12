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
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(TestS3Minio.class)
@QuarkusTestResource(TestDatabase.class)
@TestProfile(TestSparkIcebergConsumerTestResource.class)
public class TestSparkIcebergConsumer extends BaseSparkTest {


  @ConfigProperty(name = "debezium.sink.type")
  String sinkType;

  @Test
  public void simpleUploadTest() {
    Testing.Print.enable();
    Assertions.assertThat(sinkType.equals("batch"));

    Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
      try {
        Dataset<Row> df = spark.sql("select * from default.testc_inventory_customers");
        df.show(false);
        return df.filter("id is not null").count() >= 4;
      } catch (Exception e) {
        return false;
      }
    });
    TestS3Minio.listFiles();
  }

  @Test
  public void testDatatypes() throws Exception {
    String sql = "\n" +
        "        DROP TABLE IF EXISTS inventory.table_datatypes;\n" +
        "        CREATE TABLE IF NOT EXISTS inventory.table_datatypes (\n" +
        "            c_id INTEGER ,\n" +
        "            c_text TEXT,\n" +
        "            c_varchar VARCHAR,\n" +
        "            c_int INTEGER,\n" +
        "            c_date DATE,\n" +
        "            c_timestamp TIMESTAMP,\n" +
        "            c_timestamptz TIMESTAMPTZ,\n" +
        "            c_float FLOAT,\n" +
        "            c_decimal DECIMAL(18,4),\n" +
        "            c_numeric NUMERIC(18,4),\n" +
        "            c_interval INTERVAL,\n" +
        "            c_boolean BOOLean,\n" +
        "            c_uuid UUID,\n" +
        "            c_bytea BYTEA\n" +
        "          );";
    TestDatabase.runSQL(sql);
    sql = "INSERT INTO inventory.table_datatypes (" +
        "c_id, " +
        "c_text, c_varchar, c_int, c_date, c_timestamp, c_timestamptz, " +
        "c_float, c_decimal,c_numeric,c_interval,c_boolean,c_uuid,c_bytea  " +
        ") " +
        "VALUES (1, null, null, null,null,null,null," +
        "null,null,null,null,null,null,null" +
        ")," +
        "(2, 'val_text', 'A', 123, current_date , current_timestamp, current_timestamp," +
        "'1.23'::float,'1234566.34456'::decimal,'345.452'::numeric(18,4), interval '1 day',false," +
        "'3f207ac6-5dba-11eb-ae93-0242ac130002'::UUID, 'aBC'::bytea" +
        ")";
    TestDatabase.runSQL(sql);

    Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
      try {
        Dataset<Row> df = spark.sql("select * from default.testc_inventory_table_datatypes where c_id = 2");

        df = df.withColumn("c_bytea", df.col("c_bytea").cast("string"));
        df = df.withColumn("c_numeric", df.col("c_numeric").cast("float"));
        df = df.withColumn("c_float", df.col("c_float").cast("float"));
        df = df.withColumn("c_decimal", df.col("c_decimal").cast("float"));
        df.show(false);
        df.printSchema();
        return df.where("c_bytea == 'aBC' " +
            "AND c_float == '1.23'" +
            "AND c_decimal == '1234566.3446'" +
            "AND c_numeric == '345.452'" +
            // interval as milisecond
            "AND c_interval == '86400000000'" +
            "").
            count() == 1;
      } catch (Exception e) {
        if (e.getMessage().contains("cast")) {
          System.out.println(e.getMessage());
        }
        return false;
      }
    });

    // check null values
    Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
      try {
        Dataset<Row> df = spark.sql("select * from default.testc_inventory_table_datatypes");

        df.show();
        return df.where("c_text is null AND c_varchar is null AND c_int is null " +
            "AND c_date is null AND c_timestamp is null AND c_timestamptz is null " +
            "AND c_float is null AND c_decimal is null AND c_numeric is null AND c_interval is null " +
            "AND c_boolean is null AND c_uuid is null AND c_bytea is null").count() == 1;
      } catch (Exception e) {
        return false;
      }
    });
  }

  @Test
  public void testUpdateDeleteDrop() throws Exception {
    Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
      try {
        Dataset<Row> ds = spark.sql("select * from default.testc_inventory_customers");
        ds.show();
        return ds.count() >= 2;
      } catch (Exception e) {
        return false;
      }
    });

    TestDatabase.runSQL("UPDATE inventory.customers SET first_name='George__UPDATE1' WHERE ID = 1002 ;");
//    TestDatabase.runSQL("ALTER TABLE inventory.customers ADD test_varchar_column varchar(255);");
//    TestDatabase.runSQL("ALTER TABLE inventory.customers ADD test_boolean_column boolean;");
//    TestDatabase.runSQL("ALTER TABLE inventory.customers ADD test_date_column date;");

    TestDatabase.runSQL("UPDATE inventory.customers SET first_name='George__UPDATE1'  WHERE id = 1002 ;");
    TestDatabase.runSQL("ALTER TABLE inventory.customers ALTER COLUMN email DROP NOT NULL;");
    TestDatabase.runSQL("INSERT INTO inventory.customers VALUES " +
        "(default,'SallyUSer2','Thomas',null);");
    TestDatabase.runSQL("ALTER TABLE inventory.customers ALTER COLUMN last_name DROP NOT NULL;");
    TestDatabase.runSQL("UPDATE inventory.customers SET last_name = NULL  WHERE id = 1002 ;");
    TestDatabase.runSQL("DELETE FROM inventory.customers WHERE id = 1004 ;");

    Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
      try {
        Dataset<Row> ds = spark.sql("select * from default.testc_inventory_customers");
        ds.show(false);
        return ds.where("first_name == 'George__UPDATE1'").count() == 3
            && ds.where("first_name == 'SallyUSer2'").count() == 1
            && ds.where("last_name is null").count() == 1
            && ds.where("id == '1004'").where("__op == 'd'").count() == 1
            ;
      } catch (Exception e) {
        return false;
      }
    });

    spark.sql("select * from default.testc_inventory_customers").show();
    TestDatabase.runSQL("ALTER TABLE inventory.customers DROP COLUMN email;");
    TestDatabase.runSQL("INSERT INTO inventory.customers VALUES " +
        "(default,'User3','lastname_value3');");
    Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
      try {
        Dataset<Row> ds = spark.sql("select * from default.testc_inventory_customers");
        System.out.println("----------------------");
        return ds.where("first_name == 'User3'").count() == 1
            && ds.where("test_varchar_column == 'test_varchar_value3'").count() == 1;
      } catch (Exception e) {
        return false;
      }
    });

  }

}
