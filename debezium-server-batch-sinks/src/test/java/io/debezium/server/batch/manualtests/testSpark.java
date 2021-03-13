/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.manualtests;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class testSpark {
  private static final Logger LOG = LoggerFactory.getLogger(testSpark.class);

  public static void main(String[] args) {
    Logger LOGGER = LoggerFactory.getLogger(testSpark.class);

    org.apache.kafka.connect.json.JsonDeserializer test =
        new org.apache.kafka.connect.json.JsonDeserializer();

    SparkSession spark =
        SparkSession.builder()
            .appName("Java Spark SQL basic example")
            .config("spark.master", "local")
            .config("spark.io.compression.codec", "snappy")
            .getOrCreate();
    tesbadrecord(spark);
  }

  private static void tesschema2(SparkSession spark) {

    StructType schemaUntyped =
        new StructType().add("a", "int").add("b", "string").add("c", "string").add("d", "string");

    String data = "{\"id\":100, \"first_name\":\"Edward\"}";
    data += "\n{\"id\":1001, \"first_name\":\"Edward\"}";
    data += "\n{\"id\":1001, \"first_name\":\"Edward\", \"first_name2\":\"Edwardoo\"}";

    List<String> jsonData = Arrays.asList(data.split(System.lineSeparator()));
    Dataset<String> _df = spark.createDataset(jsonData, Encoders.STRING());
    Dataset<Row> df = spark.read().schema(schemaUntyped).json(_df);
    df.printSchema();
    LOG.info(String.valueOf(df.count()));
    LOG.info(String.valueOf(df.describe()));
    LOG.info(Arrays.toString(df.columns()));
    LOG.info("---------------------------------------------------------------");
  }

  private static void tesbadrecord(SparkSession spark) {

    Dataset<Row> df =
        spark
            .read()
            .option("badRecordsPath", "/tmp/badRecordsPath")
            .schema("a int, b int")
            .json("/Users/ismailsimsek/Desktop/badJson");
    df.show();
  }
}
