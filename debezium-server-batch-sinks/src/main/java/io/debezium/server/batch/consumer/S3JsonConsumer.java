/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.consumer;

import io.debezium.server.batch.cache.BatchJsonlinesFile;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Alternative;

import org.eclipse.microprofile.config.ConfigProvider;
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
public class S3JsonConsumer extends AbstractConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(S3JsonConsumer.class);

    private final S3Client s3Client;

    @ConfigProperty(name = "debezium.sink.batch.s3.bucket-name", defaultValue = "My-S3-Bucket")
    String bucket;

    @ConfigProperty(name = "debezium.sink.batch.s3.credentials.use-instance-cred", defaultValue = "false")
    Boolean useInstanceProfile;

    @ConfigProperty(name = "debezium.sink.batch.s3.region", defaultValue = "eu-central-1")
    String region;

    @ConfigProperty(name = "debezium.sink.batch.s3.endpoint-override", defaultValue = "false")
    String endpointOverride;

    @ConfigProperty(name = "debezium.sink.batch.upload-threads", defaultValue = "16")
    Integer uploadThreads;

    ThreadPoolExecutor threadPool;


  public S3JsonConsumer()
      throws URISyntaxException {
    super();

    final AwsCredentialsProvider credProvider;
    if (useInstanceProfile) {
      credProvider = InstanceProfileCredentialsProvider.create();
      LOG.info("Using Instance Profile Credentials For S3");
    } else {
      credProvider = DefaultCredentialsProvider.create();
      LOG.info("Using DefaultCredentialsProvider For S3");
    }
    S3ClientBuilder clientBuilder = S3Client.builder()
        .region(Region.of(region))
        .credentialsProvider(credProvider);
    // used for testing, using minio
    if (!endpointOverride.trim().equalsIgnoreCase("false")) {
      clientBuilder.endpointOverride(new URI(endpointOverride));
      LOG.info("Overriding S3 Endpoint with:{}", endpointOverride);
    }
    this.s3Client = clientBuilder.build();
    threadPool = new ThreadPoolExecutor(uploadThreads, uploadThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

    LOG.info("Using default S3Client '{}'", this.s3Client);
    LOG.info("Starting S3 Batch Consumer({})", this.getClass().getName());
  }

  @Override
  public void uploadDestination(String destination) {
    String s3File = map(destination) + "/" + UUID.randomUUID() + ".json";
    BatchJsonlinesFile tempFile = this.cache.getJsonLines(destination);
    if (tempFile == null) {
      return;
    }
    LOG.info("Uploading s3File bucket:{} file:{} destination:{} key:{}", bucket, tempFile.getFile().getAbsolutePath(),
        destination, s3File);
    final PutObjectRequest putRecord = PutObjectRequest.builder()
        .bucket(bucket)
        .key(s3File)
        .build();
    s3Client.putObject(putRecord, RequestBody.fromFile(tempFile.getFile().toPath()));
    tempFile.getFile().delete();
    LOG.info("Upload Succeeded! destination:{} key:{}", destination, s3File);
  }

  @Override
  public void close() throws IOException {
    this.stopTimerUpload();
    this.stopUploadQueue();
    cache.close();
  }
}
