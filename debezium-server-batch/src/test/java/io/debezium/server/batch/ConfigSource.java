/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.batch;

import io.debezium.server.TestConfigSource;
import io.debezium.server.TestDatabase;
import io.debezium.server.testresource.TestS3Minio;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

public class ConfigSource extends TestConfigSource {

  public static final String S3_REGION = "us-east-1";
  public static final String S3_BUCKET = "test-bucket";

  final Map<String, String> s3Test = new HashMap<>();

  public ConfigSource() {
    // common conf
    s3Test.put("debezium.sink.type", "batch");
    s3Test.put("debezium.sink.batch.objectkey.prefix", "debezium-cdc-");
    // s3Test.put("debezium.sink.batch.objectkey.mapper", "dailypartitioned");
    // s3Test.put("debezium.sink.batch.batchwriter", "s3json");
    s3Test.put("debezium.sink.batch.objectkey.mapper", "table");
    s3Test.put("debezium.sink.batch.batchwriter", "iceberg");
    s3Test.put("debezium.sink.batch.row.limit", "2");
    s3Test.put("debezium.sink.batch.time.limit", "3600");

    // s3batch sink
    s3Test.put("debezium.sink.batch.s3.region", S3_REGION);
    s3Test.put("debezium.sink.batch.s3.endpointoverride", "http://localhost:9000");
    s3Test.put("debezium.sink.batch.s3.bucket.name", "s3a://" + S3_BUCKET);
    s3Test.put("debezium.sink.batch.s3.credentials.useinstancecred", "false");

    // sparkbatch sink conf
    s3Test.put("debezium.sink.sparkbatch.removeschema", "true");
    s3Test.put("debezium.sink.sparkbatch.saveformat", "parquet");
    s3Test.put("debezium.sink.sparkbatch.bucket.name", "s3a://" + S3_BUCKET);

    // iceberg
    s3Test.put("debezium.sink.iceberg.fs.defaultFS", "s3a://" + S3_BUCKET);
    s3Test.put("debezium.sink.iceberg.warehouse", "s3a://" + S3_BUCKET + "/iceberg_warehouse");
    s3Test.put("debezium.sink.iceberg.user.timezone", "UTC");
    s3Test.put("debezium.sink.iceberg.com.amazonaws.services.s3.enableV4", "true");
    s3Test.put("debezium.sink.iceberg.com.amazonaws.services.s3a.enableV4", "true");
    s3Test.put("debezium.sink.iceberg.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
    s3Test.put("debezium.sink.iceberg.fs.s3a.access.key", TestS3Minio.MINIO_ACCESS_KEY);
    s3Test.put("debezium.sink.iceberg.fs.s3a.secret.key", TestS3Minio.MINIO_SECRET_KEY);
    s3Test.put("debezium.sink.iceberg.fs.s3a.path.style.access", "true");
    s3Test.put("debezium.sink.iceberg.fs.s3a.endpoint", "http://localhost:9000"); // minio specific setting
    s3Test.put("debezium.sink.iceberg.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

    // spark conf
    s3Test.put("debezium.sink.sparkbatch.spark.ui.enabled", "false");
    s3Test.put("debezium.sink.sparkbatch.spark.sql.session.timeZone", "UTC");
    s3Test.put("debezium.sink.sparkbatch.user.timezone", "UTC");
    s3Test.put("debezium.sink.sparkbatch.com.amazonaws.services.s3.enableV4", "true");
    s3Test.put("debezium.sink.sparkbatch.com.amazonaws.services.s3a.enableV4", "true");
    s3Test.put("debezium.sink.sparkbatch.spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true");
    s3Test.put("debezium.sink.sparkbatch.spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true");
    s3Test.put("debezium.sink.sparkbatch.spark.io.compression.codec", "snappy");
    s3Test.put("debezium.sink.sparkbatch.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
    // endpoint override or testing
    s3Test.put("debezium.sink.sparkbatch.spark.hadoop.fs.s3a.access.key", TestS3Minio.MINIO_ACCESS_KEY);
    s3Test.put("debezium.sink.sparkbatch.spark.hadoop.fs.s3a.secret.key", TestS3Minio.MINIO_SECRET_KEY);
    s3Test.put("debezium.sink.sparkbatch.spark.hadoop.fs.s3a.path.style.access", "true");
    s3Test.put("debezium.sink.sparkbatch.spark.hadoop.fs.s3a.endpoint", "http://localhost:9000"); // minio specific setting
    s3Test.put("debezium.sink.sparkbatch.spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    s3Test.put("debezium.sink.sparkbatch.spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    s3Test.put("debezium.sink.sparkbatch.spark.sql.catalog.spark_catalog.type", "hadoop");
    s3Test.put("debezium.sink.sparkbatch.spark.sql.catalog.spark_catalog.warehouse", "s3a://" + S3_BUCKET + "/iceberg_warehouse");

    // DEBEZIUM PROP
    s3Test.put("debezium.sink.sparkbatch.spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore");
    // enable disable schema
    s3Test.put("debezium.format.value.schemas.enable", "true");
    // s3Test.put("debezium.format.value.converter", "io.debezium.converters.CloudEventsConverter");
    // s3Test.put("value.converter", "io.debezium.converters.CloudEventsConverter");
    // s3Test.put("debezium.format.value.converter.data.serializer.type" , "json");
    // s3Test.put("value.converter.data.serializer.type", "json");

    // debezium unwrap message
    s3Test.put("debezium.transforms", "unwrap");
    s3Test.put("debezium.transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
    s3Test.put("debezium.transforms.unwrap.add.fields", "op,table,lsn,source.ts_ms");
    s3Test.put("debezium.transforms.unwrap.add.headers", "db");
    s3Test.put("debezium.transforms.unwrap.delete.handling.mode", "rewrite");

    // DEBEZIUM SOURCE conf
    s3Test.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
    s3Test.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
    s3Test.put("debezium.source.offset.flush.interval.ms", "0");
    s3Test.put("debezium.source.database.hostname", TestDatabase.POSTGRES_HOST);
    // this set by TestDatabase
    s3Test.put("debezium.source.database.port", Integer.toString(5432));
    s3Test.put("debezium.source.database.user", TestDatabase.POSTGRES_USER);
    s3Test.put("debezium.source.database.password", TestDatabase.POSTGRES_PASSWORD);
    s3Test.put("debezium.source.database.dbname", TestDatabase.POSTGRES_DBNAME);
    s3Test.put("debezium.source.database.server.name", "testc");
    s3Test.put("debezium.source.schema.whitelist", "inventory");
    s3Test.put("debezium.source.table.whitelist", "inventory.customers,inventory.orders");

    config = s3Test;
  }
}
