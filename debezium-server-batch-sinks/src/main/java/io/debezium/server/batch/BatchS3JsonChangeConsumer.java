/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.engine.ChangeEvent;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.UUID;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import org.eclipse.microprofile.config.inject.ConfigProperty;
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
@Named("s3jsonbatch")
@Dependent
public class BatchS3JsonChangeConsumer extends AbstractBatchChangeConsumer {

  @Inject
  protected ObjectStorageNameMapper objectStorageNameMapper;
  @ConfigProperty(name = "debezium.sink.batch.s3.bucket-name", defaultValue = "My-S3-Bucket")
  String bucket;
  @ConfigProperty(name = "debezium.sink.batch.s3.credentials-use-instance-cred", defaultValue = "false")
  Boolean useInstanceProfile;
  @ConfigProperty(name = "debezium.sink.batch.s3.region", defaultValue = "eu-central-1")
  String region;
  @ConfigProperty(name = "debezium.sink.batch.s3.endpoint-override", defaultValue = "false")
  String endpointOverride;
  private S3Client s3Client;

  @PreDestroy
  void close() {
    s3Client.close();
  }

  @PostConstruct
  void connect() throws URISyntaxException, InterruptedException {
    super.initizalize();

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
  public long uploadDestination(String destination, ArrayList<ChangeEvent<Object, Object>> data) {
    JsonlinesBatchFile jsonLinesFile = this.getJsonLines(destination, data);
    String s3File = objectStorageNameMapper.map(destination) + "/" + UUID.randomUUID() + ".json";
    if (jsonLinesFile == null) {
      return 0L;
    }
    LOGGER.debug("Uploading s3File bucket:{} file:{} destination:{} key:{}", bucket,
        jsonLinesFile.getFile().getAbsolutePath(),
        destination, s3File);
    final PutObjectRequest putRecord = PutObjectRequest.builder()
        .bucket(bucket)
        .key(s3File)
        .build();
    s3Client.putObject(putRecord, RequestBody.fromFile(jsonLinesFile.getFile().toPath()));
    jsonLinesFile.getFile().delete();
    LOGGER.debug("Upload Succeeded! destination:{} key:{}", destination, s3File);
    return jsonLinesFile.getNumLines();
  }

}
