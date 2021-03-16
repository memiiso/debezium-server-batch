/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.writer;

import io.debezium.server.batch.BatchJsonlinesFile;
import io.debezium.server.batch.ObjectStorageNameMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Alternative;
import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
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

@Dependent
@Alternative
public class S3JsonWriter implements BatchWriter {
  protected static final Logger LOGGER = LoggerFactory.getLogger(S3JsonWriter.class);

  @ConfigProperty(name = "debezium.sink.batch.s3.bucket-name", defaultValue = "My-S3-Bucket")
  String bucket;

  @ConfigProperty(name = "debezium.sink.batch.s3.credentials-use-instance-cred", defaultValue = "false")
  Boolean useInstanceProfile;

  @ConfigProperty(name = "debezium.sink.batch.s3.region", defaultValue = "eu-central-1")
  String region;

  @ConfigProperty(name = "debezium.sink.batch.s3.endpoint-override", defaultValue = "false")
  String endpointOverride;

  private S3Client s3Client;

  @Inject
  protected ObjectStorageNameMapper objectStorageNameMapper;


  public S3JsonWriter() {
  }


  @Override
  public void initialize() {

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
    if (!endpointOverride.trim().equalsIgnoreCase("false")) {
      LOGGER.info("Overriding S3 Endpoint with:{}", endpointOverride);
      try {
        clientBuilder.endpointOverride(new URI(endpointOverride));
      } catch (URISyntaxException e) {
        throw new RuntimeException("Failed to override S3 endpoint", e);
      }
    }
    this.s3Client = clientBuilder.build();

    LOGGER.info("Using default S3Client '{}'", this.s3Client);
    LOGGER.info("Starting S3 Batch Consumer({})", this.getClass().getName());
  }

  @Override
  public void uploadDestination(String destination, BatchJsonlinesFile jsonLinesFile) {
    String s3File = objectStorageNameMapper.map(destination) + "/" + UUID.randomUUID() + ".json";
    if (jsonLinesFile == null) {
      return;
    }
    LOGGER.info("Uploading s3File bucket:{} file:{} destination:{} key:{}", bucket, jsonLinesFile.getFile().getAbsolutePath(),
        destination, s3File);
    final PutObjectRequest putRecord = PutObjectRequest.builder()
        .bucket(bucket)
        .key(s3File)
        .build();
    s3Client.putObject(putRecord, RequestBody.fromFile(jsonLinesFile.getFile().toPath()));
    jsonLinesFile.getFile().delete();
    LOGGER.info("Upload Succeeded! destination:{} key:{}", destination, s3File);
  }

  @Override
  public void close() throws IOException {
  }
}
