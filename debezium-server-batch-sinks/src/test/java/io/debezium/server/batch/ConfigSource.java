/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.server.TestConfigSource;
import io.debezium.server.batch.common.S3Minio;
import io.debezium.server.batch.common.SourcePostgresqlDB;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

public class ConfigSource extends TestConfigSource {

  public static final String S3_REGION = "us-east-1";
  public static final String S3_BUCKET = "test-bucket";

  final Map<String, String> s3Test = new HashMap<>();

  public ConfigSource() {
    // common sink conf
    s3Test.put("debezium.sink.type", "sparkbatch");
    //s3Test.put("quarkus.arc.selected-alternatives", "SparkWriter,MemoryCache");
    s3Test.put("debezium.sink.batch.objectkey-prefix", "debezium-cdc-");
    s3Test.put("debezium.sink.batch.objectkey-partition", "true");
    s3Test.put("debezium.sink.batch.row-limit", "2");
    s3Test.put("debezium.sink.batch.time-limit", "10"); // second

    s3Test.put("debezium.source.max.batch.size", "1");
    s3Test.put("debezium.source.poll.interval.ms", "5000");

    s3Test.put("debezium.source.database.history.kafka.bootstrap.servers", "kafka:9092");
    s3Test.put("debezium.source.database.history.kafka.topic", "dbhistory.fullfillment");
    s3Test.put("debezium.source.include.schema.changes", "false");
    s3Test.put("debezium.source.database.history", "io.debezium.relational.history.FileDatabaseHistory");
    s3Test.put("debezium.source.database.history.file.filename", "data/dbhistory.txt");

    // cache
    // sparkbatch sink conf
    s3Test.put("debezium.sink.sparkbatch.save-format", "parquet");
    s3Test.put("debezium.sink.sparkbatch.bucket-name", "s3a://" + S3_BUCKET);
    s3Test.put("debezium.sink.batch.cache.purge-on-startup", "true");
    // spark conf
    s3Test.put("debezium.sink.sparkbatch.spark.ui.enabled", "false");
    s3Test.put("debezium.sink.sparkbatch.spark.sql.session.timeZone", "UTC");
    s3Test.put("debezium.sink.sparkbatch.user.timezone", "UTC");
    s3Test.put("debezium.sink.sparkbatch.spark.io.compression.codec", "snappy");
    // endpoint override or testing
    s3Test.put("debezium.sink.sparkbatch.fs.s3a.access.key", S3Minio.MINIO_ACCESS_KEY);
    s3Test.put("debezium.sink.sparkbatch.fs.s3a.secret.key", S3Minio.MINIO_SECRET_KEY);
    s3Test.put("debezium.sink.sparkbatch.spark.hadoop.fs.s3a.path.style.access", "true");
    s3Test.put("debezium.sink.sparkbatch.spark.hadoop.fs.s3a.endpoint", "http://localhost:9000"); // minio specific setting

    // DEBEZIUM PROP
    // enable disable schema
    s3Test.put("debezium.format.value.schemas.enable", "true");

    // debezium unwrap message
    s3Test.put("debezium.transforms", "unwrap");
    s3Test.put("debezium.transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
    s3Test.put("debezium.transforms.unwrap.add.fields", "op,table,source.ts_ms");
    s3Test.put("debezium.transforms.unwrap.add.headers", "db");
    s3Test.put("debezium.transforms.unwrap.delete.handling.mode", "rewrite");

    // DEBEZIUM SOURCE conf
    s3Test.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
    s3Test.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
    s3Test.put("debezium.source.offset.flush.interval.ms", "60000");
    s3Test.put("debezium.source.database.hostname", SourcePostgresqlDB.POSTGRES_HOST);
    // this set by SourcePostgresqlDB
    s3Test.put("debezium.source.database.port", Integer.toString(5432));
    s3Test.put("debezium.source.database.user", SourcePostgresqlDB.POSTGRES_USER);
    s3Test.put("debezium.source.database.password", SourcePostgresqlDB.POSTGRES_PASSWORD);
    s3Test.put("debezium.source.database.dbname", SourcePostgresqlDB.POSTGRES_DBNAME);
    s3Test.put("debezium.source.database.server.name", "testc");
    s3Test.put("debezium.source.schema.include.list", "inventory");
    s3Test.put("debezium.source.table.include.list", "inventory.customers,inventory.orders,inventory.products," +
        "inventory.dummy_performance_table," +
        "inventory.geom,inventory.table_datatypes");

//    However, when decimal.handling.mode configuration property is set to double, then the connector will represent
//    all DECIMAL and NUMERIC values as Java double values and encodes them as follows:
    s3Test.put("debezium.source.decimal.handling.mode", "double");

    s3Test.put("quarkus.log.level", "INFO");
    s3Test.put("quarkus.log.category.\"org.apache.spark\".level", "WARN");
    s3Test.put("quarkus.log.category.\"org.apache.hadoop\".level", "ERROR");
    s3Test.put("quarkus.log.category.\"org.apache.parquet\".level", "WARN");
    s3Test.put("quarkus.log.category.\"org.eclipse.jetty\".level", "WARN");

    config = s3Test;
  }
}
