/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class BatchSparkChangeConsumerMysqlTestProfile implements QuarkusTestProfile {

  //This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();
    config.put("quarkus.profile", "mysql");
    config.put("%mysql.debezium.source.connector.class", "io.debezium.connector.mysql.MySqlConnector");
    config.put("debezium.source.max.batch.size", "500");
    config.put("debezium.source.max.queue.size", "70000");
    // 30000 30-second
    config.put("debezium.source.poll.interval.ms", "1000");
    config.put("debezium.sink.sparkbatch.spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory");
    config.put("debezium.sink.sparkbatch.spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol");
    config.put("debezium.sink.sparkbatch.spark.sql.parquet.output.committer.class", "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter");
    config.put("debezium.sink.sparkbatch.spark.hadoop.fs.s3a.committer.name", "magic");
    config.put("debezium.sink.sparkbatch.spark.hadoop.fs.s3a.committer.magic.enabled", "true");

    return config;
  }

  @Override
  public String getConfigProfile() {
    return "mysql";
  }

}
