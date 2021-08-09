/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.engine.ChangeEvent;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import com.fasterxml.jackson.databind.JsonNode;
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

  public JsonlinesBatchFile getJsonLines(String destination, List<ChangeEvent<Object, Object>> data) {

    Instant start = Instant.now();
    JsonNode valSchema = null;
    JsonNode keySchema = null;
    boolean isFirst = true;
    final File tempFile;
    long numLines = 0L;
    try {
      tempFile = File.createTempFile(UUID.randomUUID() + "-", ".json");
      FileOutputStream fos = new FileOutputStream(tempFile, true);
      LOGGER.debug("Writing {} events as jsonlines file: {}", data.size(), tempFile);

      for (ChangeEvent<Object, Object> e : data) {
        Object val = e.value();
        Object key = e.key();

        // this could happen if multiple threads reading and removing data
        if (val == null) {
          LOGGER.warn("Cache.getJsonLines Null Event Value found for destination:'{}'! " +
              "skipping the entry!", destination);
          continue;
        }
        LOGGER.trace("Cache.getJsonLines val:{}", getString(val));

        if (isFirst) {
          valSchema = BatchUtil.getJsonSchemaNode(getString(val));
          if (key != null) {
            keySchema = BatchUtil.getJsonSchemaNode(getString(key));
          }
          isFirst = false;
        }

        try {
          final JsonNode valNode = valDeserializer.deserialize(destination, getBytes(val));
          final String valData = mapper.writeValueAsString(valNode) + System.lineSeparator();

          if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Cache.getJsonLines val Json Node:{}", valNode.toString());
            LOGGER.trace("Cache.getJsonLines val String:{}", valData);
          }

          fos.write(valData.getBytes(StandardCharsets.UTF_8));
          numLines++;
        } catch (IOException ioe) {
          LOGGER.error("Failed writing record to file", ioe);
          fos.close();
          throw new UncheckedIOException(ioe);
        }
      }

      fos.close();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    LOGGER.trace("Writing jsonlines file took:{}",
        Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS));

    // if nothing processed return null
    if (isFirst) {
      tempFile.delete();
      return null;
    }

    return new JsonlinesBatchFile(tempFile, valSchema, keySchema, numLines);
  }


  @Override
  public long uploadDestination(String destination, List<ChangeEvent<Object, Object>> data) {
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
