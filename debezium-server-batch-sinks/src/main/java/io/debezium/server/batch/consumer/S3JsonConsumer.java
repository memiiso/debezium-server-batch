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
  protected static final Logger LOGGER = LoggerFactory.getLogger(S3JsonConsumer.class);
  protected static final String bucket = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.s3" +
      ".bucket-name", String.class).orElse("My-S3-Bucket");
  protected static final Boolean useInstanceProfile = ConfigProvider.getConfig().getOptionalValue("debezium.sink" +
      ".batch.s3.credentials.use-instance-cred", Boolean.class)
      .orElse(false);
  final static String region = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.s3.region",
      String.class).orElse("eu-central-1");
  final static String endpointOverride = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.s3" +
      ".endpoint-override", String.class).orElse("false");
  private final S3Client s3Client;

  final static Integer uploadThreads =
      ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.upload-threads", Integer.class).orElse(16);
  ThreadPoolExecutor threadPool;


  public S3JsonConsumer()
      throws URISyntaxException {
    super();

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
      LOGGER.info("Overriding S3 Endpoint with:{}", endpointOverride);
    }
    this.s3Client = clientBuilder.build();
    threadPool = new ThreadPoolExecutor(uploadThreads, uploadThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

    LOGGER.info("Using default S3Client '{}'", this.s3Client);
    LOGGER.info("Starting S3 Batch Consumer({})", this.getClass().getName());
  }

  @Override
  public void uploadDestination(String destination, String uploadTrigger) {
    int poolSize = threadPool.getPoolSize();
    int active = threadPool.getActiveCount();
    long submitted = threadPool.getTaskCount();
    long completed = threadPool.getCompletedTaskCount();

    LOGGER.info("Upload poolSize:{}, active:{}, waiting:{}, total submitted:{}, total completed:{}, not completed:{}",
        poolSize,
        active,
        poolSize - active, submitted,
        completed, submitted - completed);
    // use thread pool to limit parallel runs
    Thread uploadThread = new Thread(() -> {
      Thread.currentThread().setName("s3json-" + uploadTrigger + "-upload-" + Thread.currentThread().getId());

      String s3File = map(destination) + "/" + UUID.randomUUID() + ".json";
      BatchJsonlinesFile tempFile = this.cache.getJsonLines(destination);
      if (tempFile == null) {
        return;
      }
      LOGGER.info("Uploading s3File bucket:{} file:{} destination:{} key:{}", bucket, tempFile.getFile().getAbsolutePath(),
          destination, s3File);
      final PutObjectRequest putRecord = PutObjectRequest.builder()
          .bucket(bucket)
          .key(s3File)
          .build();
      s3Client.putObject(putRecord, RequestBody.fromFile(tempFile.getFile().toPath()));
      tempFile.getFile().delete();
      LOGGER.info("Upload Succeeded! destination:{} key:{}", destination, s3File);

    });
    threadPool.submit(uploadThread);
  }

  @Override
  public void close() throws IOException {
    this.stopTimerUpload();
    cache.close();
  }
}
