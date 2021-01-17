/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.writer;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

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
public class S3JsonWriter extends AbstractBatchWriter {
  protected static final Logger LOGGER = LoggerFactory.getLogger(S3JsonWriter.class);
  protected final String bucket = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.s3.bucket-name", String.class).orElse("My-S3-Bucket");
  protected final Boolean useInstanceProfile = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.s3.credentials.use-instance-cred", Boolean.class)
      .orElse(false);
  final String region = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.s3.region", String.class).orElse("eu-central-1");
  final String endpointOverride = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.s3.endpoint-override", String.class).orElse("false");
  private final S3Client s3Client;

  public S3JsonWriter()
      throws URISyntaxException {
    super();
    // @TODO enforce format to be json

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
      clientBuilder.endpointOverride(new URI(endpointOverride));
    }
    this.s3Client = clientBuilder.build();
    LOGGER.info("Using default S3Client '{}'", this.s3Client);
    LOGGER.info("Starting S3 Batch Consumer({})", this.getClass().getName());
  }

  @Override
  public void uploadOne(String destination) {
    String s3File = map(destination) + "/" + UUID.randomUUID() + ".json";
    File tempFile = this.getJsonLines(destination);
    if (tempFile == null) {
      return;
    }
    LOGGER.info("Uploading s3File bucket:{} file:{} destination:{} key:{}", bucket, tempFile.getAbsolutePath(),
        destination, s3File);
    final PutObjectRequest putRecord = PutObjectRequest.builder()
        .bucket(bucket)
        .key(s3File)
        .build();
    s3Client.putObject(putRecord, RequestBody.fromFile(tempFile.toPath()));
    tempFile.delete();
    LOGGER.info("Upload Succeeded! destination:{} key:{}", destination, s3File);
  }

}
