/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.s3;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.batch.ObjectStorageNameMapper;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("s3")
@Dependent
public class S3ChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3ChangeConsumer.class);

  @ConfigProperty(name = "debezium.sink.s3.credentials.profile", defaultValue = "default")
  String credentialsProfile;

  @ConfigProperty(name = "debezium.sink.s3.endpoint-override", defaultValue = "false")
  String endpointOverride;

  @ConfigProperty(name = "debezium.sink.s3.credentials.use-instance-cred", defaultValue = "false")
  Boolean useInstanceProfile;

  @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
  String valueFormat;

  S3Client s3client;

  @ConfigProperty(name = "debezium.sink.s3.bucket-name", defaultValue = "My-S3-Bucket")
  String bucket;

  @ConfigProperty(name = "debezium.sink.s3.region", defaultValue = "eu-central-1")
  String region;

  @Inject
  protected ObjectStorageNameMapper objectStorageNameMapper;

  @PostConstruct
  void connect() throws URISyntaxException {

    AwsCredentialsProvider credProvider;
    if (useInstanceProfile) {
      LOGGER.info("Using Instance Profile Credentials For S3");
      credProvider = InstanceProfileCredentialsProvider.create();
    } else {
      credProvider = ProfileCredentialsProvider.create(credentialsProfile);
    }

    S3ClientBuilder clientBuilder = S3Client.builder()
        .region(Region.of(region))
        .credentialsProvider(credProvider);
    // used for testing, using minio
    if (!endpointOverride.trim().equalsIgnoreCase("false")) {
      clientBuilder.endpointOverride(new URI(endpointOverride));
      LOGGER.info("Overriding S3 Endpoint with:{}", endpointOverride);
    }
    s3client = clientBuilder.build();
    LOGGER.info("Using default S3Client '{}'", s3client);
  }

  @PreDestroy
  void close() {
    try {
      s3client.close();
    } catch (Exception e) {
      LOGGER.error("Exception while closing S3 client: ", e);
    }
  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {
    LocalDateTime batchTime = LocalDateTime.now();
    for (ChangeEvent<Object, Object> record : records) {
      final String fname = batchTime.toEpochSecond(ZoneOffset.UTC) + UUID.randomUUID().toString() + "." + valueFormat;
      PutObjectRequest putRecord = PutObjectRequest.builder()
          .bucket(bucket)
          .key(objectStorageNameMapper.map(record.destination()) + "/" + fname)
          .build();
      LOGGER.debug("Uploading s3File bucket:{} key:{} endpint:{}", putRecord.bucket(), putRecord.key(), endpointOverride);
      s3client.putObject(putRecord, RequestBody.fromBytes(getBytes(record.value())));
      committer.markProcessed(record);
    }
    committer.markBatchFinished();
  }
}
