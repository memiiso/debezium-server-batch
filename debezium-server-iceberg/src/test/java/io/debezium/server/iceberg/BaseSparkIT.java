/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import io.debezium.util.Testing;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.BeforeAll;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
public class BaseSparkIT {
  protected static final SparkConf sparkconf = new SparkConf()
      .setAppName("CDC-S3-Batch-Spark-Sink")
      .setMaster("local");
  private static final String SPARK_PROP_PREFIX = "debezium.sink.sparkbatch.";
  protected static SparkSession spark;
  protected String saveFormat = ConfigProvider.getConfig().getOptionalValue("debezium.sink.sparkbatch.saveformat", String.class).orElse("json");

  {
    // Testing.Debug.enable();
    Testing.Files.delete(ConfigSource.OFFSET_STORE_PATH);
    Testing.Files.createTestingFile(ConfigSource.OFFSET_STORE_PATH);
  }

  @BeforeAll
  static void setup() {

    for (String name : ConfigProvider.getConfig().getPropertyNames()) {
      if (name.startsWith(SPARK_PROP_PREFIX)) {
        BaseSparkIT.sparkconf.set(name.substring(SPARK_PROP_PREFIX.length()), ConfigProvider.getConfig().getValue(name, String.class));
      }
    }
    BaseSparkIT.sparkconf.set("spark.ui.enabled", "false");

    BaseSparkIT.spark = SparkSession
        .builder()
        .config(BaseSparkIT.sparkconf)
        .getOrCreate();
  }

}
