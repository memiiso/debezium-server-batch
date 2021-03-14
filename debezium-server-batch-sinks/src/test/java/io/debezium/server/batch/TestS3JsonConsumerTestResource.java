/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import static io.debezium.server.batch.BatchTestConfigSource.S3_BUCKET;
import static io.debezium.server.batch.BatchTestConfigSource.S3_REGION;

import java.util.HashMap;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

public class TestS3JsonConsumerTestResource implements QuarkusTestProfile {

  // This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    config.put("debezium.sink.type", "batch");
    config.put("debezium.sink.batch.writer", "s3json");
    config.put("debezium.sink.batch.s3.region", S3_REGION);
    config.put("debezium.sink.batch.s3.endpoint-override", "http://localhost:9000");
    config.put("debezium.sink.batch.s3.bucket-name", "s3a://" + S3_BUCKET);
    config.put("debezium.sink.batch.s3.credentials.use-instance-cred", "false");
    return config;
  }
}