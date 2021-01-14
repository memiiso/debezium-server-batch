/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.batch.batchwriter;

import io.debezium.server.batch.keymapper.ObjectKeyMapper;

import java.net.URI;
import java.net.URISyntaxException;

import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
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
public class S3JsonBatchRecordWriter extends AbstractBatchRecordWriter {
  protected static final Logger LOGGER = LoggerFactory.getLogger(S3JsonBatchRecordWriter.class);
  protected final String bucket = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.s3.bucket.name", String.class).orElse("My-S3-Bucket");
  protected final Boolean useInstanceProfile = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.s3.credentials.useinstancecred", Boolean.class)
      .orElse(false);
  final String region = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.s3.region", String.class).orElse("eu-central-1");
  final String endpointOverride = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.s3.endpointoverride", String.class).orElse("false");
  private final S3Client s3Client;

  public S3JsonBatchRecordWriter(ObjectKeyMapper mapper)
      throws URISyntaxException {
    super(mapper);

    final AwsCredentialsProvider credProvider;
    if (useInstanceProfile) {
      credProvider = InstanceProfileCredentialsProvider.create();
      LOGGER.info("Using Instance Profile Credentials For S3");
    } else {
      credProvider = DefaultCredentialsProvider.create();
      LOGGER.info("Using DefaultCredentialsProvider For S3");
    }
    S3ClientBuilder clientBuilder = S3Client.builder()
        .region(Region.of(region))
        .credentialsProvider(credProvider);
    // used for testing, using minio
    if (!endpointOverride.trim().toLowerCase().equals("false")) {
      clientBuilder.endpointOverride(new URI(endpointOverride));
    }
    this.s3Client = clientBuilder.build();
    LOGGER.info("Using default S3Client '{}'", this.s3Client);
    LOGGER.info("Starting S3 Batch Consumer({})", this.getClass().getName());
  }

  protected void uploadBatchFile(String destination) {
    Integer batchId = map_batchid.get(destination);
    final String data = map_data.get(destination);
    String s3File = objectKeyMapper.map(destination, batchTime, batchId);
    LOGGER.debug("Uploading s3File bucket:{} destination:{} key:{}", bucket, destination, s3File);
    final PutObjectRequest putRecord = PutObjectRequest.builder()
        .bucket(bucket)
        .key(s3File)
        .build();
    s3Client.putObject(putRecord, RequestBody.fromString(data));
    // increment batch id
    map_batchid.put(destination, batchId + 1);
    // start new batch
    map_data.remove(destination);
    cdcDb.commit();
    LOGGER.debug("Upload Succeeded! destination:{} key:{}", destination, s3File);
  }

}
