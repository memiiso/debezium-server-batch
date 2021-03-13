/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.common;

import static io.debezium.server.batch.common.TestUtil.randomInt;
import static io.debezium.server.batch.common.TestUtil.randomString;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.BeforeAll;

import io.debezium.server.batch.BatchTestConfigSource;
import io.debezium.util.Testing;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3
 * destination.
 *
 * @author Ismail Simsek
 */
public class BaseSparkTest {
  protected static final SparkConf sparkconf =
      new SparkConf().setAppName("CDC-S3-Batch-Spark-Sink").setMaster("local");
  private static final String SPARK_PROP_PREFIX = "debezium.sink.sparkbatch.";
  protected static SparkSession spark;

  static {
    Testing.Files.delete(BatchTestConfigSource.OFFSET_STORE_PATH);
    Testing.Files.createTestingFile(BatchTestConfigSource.OFFSET_STORE_PATH);
  }

  @ConfigProperty(name = "debezium.sink.batch.objectkey-prefix", defaultValue = "")
  String objectKeyPrefix;

  @ConfigProperty(name = "debezium.sink.sparkbatch.bucket-name", defaultValue = "")
  String bucket;

  @BeforeAll
  static void setup() {

    for (String name : ConfigProvider.getConfig().getPropertyNames()) {
      if (name.startsWith(SPARK_PROP_PREFIX)) {
        BaseSparkTest.sparkconf.set(
            name.substring(SPARK_PROP_PREFIX.length()),
            ConfigProvider.getConfig().getValue(name, String.class));
      }
    }
    BaseSparkTest.sparkconf.set("spark.ui.enabled", "false");

    BaseSparkTest.spark = SparkSession.builder().config(BaseSparkTest.sparkconf).getOrCreate();
  }

  public static void createDummyPerformanceTable() throws Exception {
    // create test table
    String sql =
        "\n"
            + "        CREATE TABLE IF NOT EXISTS inventory.dummy_performance_table (\n"
            + "            c_id INTEGER ,\n"
            + "            c_text TEXT,\n"
            + "            c_varchar VARCHAR"
            + "          );";
    TestDatabase.runSQL(sql);
  }

  public static int loadDataToDummyPerformanceTable(int numRows) throws Exception {
    int numInsert = 0;
    do {
      String sql =
          "INSERT INTO inventory.dummy_performance_table (c_id, c_text, c_varchar ) " + "VALUES ";
      StringBuilder values =
          new StringBuilder(
              "\n("
                  + randomInt(15, 32)
                  + ", '"
                  + randomString(524)
                  + "', '"
                  + randomString(524)
                  + "')");
      for (int i = 0; i < 10; i++) {
        values
            .append("\n,(")
            .append(randomInt(15, 32))
            .append(", '")
            .append(randomString(524))
            .append("', '")
            .append(randomString(524))
            .append("')");
      }
      TestDatabase.runSQL(sql + values);
      TestDatabase.runSQL("COMMIT;");
      numInsert += 10;
    } while (numInsert <= numRows);
    return numInsert;
  }

  public Dataset<Row> getTableData(String table) {
    return spark
        .read()
        .option("mergeSchema", "true")
        .parquet(bucket + "/" + objectKeyPrefix + table + "/*")
        .withColumn("input_file", functions.input_file_name());
  }
}
