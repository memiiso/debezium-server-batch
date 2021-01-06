/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.event.s3;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.server.BaseChangeConsumer;

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
    final String credentialsProfile = ConfigProvider.getConfig().getOptionalValue("debezium.sink.s3.credentials.profile", String.class).orElse("default");
    final String endpointOverride = ConfigProvider.getConfig().getOptionalValue("debezium.sink.s3.endpointoverride", String.class).orElse("false");
    final Boolean useInstanceProfile = ConfigProvider.getConfig().getOptionalValue("debezium.sink.s3.credentials.useinstancecred", Boolean.class).orElse(false);
    final String objectKeyPrefix = ConfigProvider.getConfig().getValue("debezium.sink.s3.objectkey.prefix", String.class);
    final String valueFormat = ConfigProvider.getConfig().getOptionalValue("debezium.format.value", String.class).orElse(Json.class.getSimpleName().toLowerCase());
    S3Client s3client;
    @ConfigProperty(name = "debezium.sink.s3.bucket.name", defaultValue = "My-S3-Bucket")
    String bucket;
    @ConfigProperty(name = "debezium.sink.s3.region", defaultValue = "eu-central-1")
    String region;

    public String map(String destination, LocalDateTime batchTime) {
        Objects.requireNonNull(destination, "destination Cannot be Null");
        Objects.requireNonNull(batchTime, "batchTime Cannot be Null");
        String fname = batchTime.toEpochSecond(ZoneOffset.UTC) + UUID.randomUUID().toString() + "." + valueFormat;
        String partiton = "year=" + batchTime.getYear() + "/month=" + StringUtils.leftPad(batchTime.getMonthValue() + "", 2, '0') + "/day="
                + StringUtils.leftPad(batchTime.getDayOfMonth() + "", 2, '0');
        return objectKeyPrefix + destination + "/" + partiton + "/" + fname;
    }

    @PostConstruct
    void connect() throws URISyntaxException {

        AwsCredentialsProvider credProvider;
        if (useInstanceProfile) {
            LOGGER.info("Using Instance Profile Credentials For S3");
            credProvider = InstanceProfileCredentialsProvider.create();
        }
        else {
            credProvider = ProfileCredentialsProvider.create(credentialsProfile);
        }

        S3ClientBuilder clientBuilder = S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(credProvider);
        // used for testing, using minio
        if (!endpointOverride.trim().toLowerCase().equals("false")) {
            clientBuilder.endpointOverride(new URI(endpointOverride));
        }
        s3client = clientBuilder.build();
        LOGGER.info("Using default S3Client '{}'", s3client);
    }

    @PreDestroy
    void close() {
        try {
            s3client.close();
        }
        catch (Exception e) {
            LOGGER.error("Exception while closing S3 client: ", e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        LocalDateTime batchTime = LocalDateTime.now();
        for (ChangeEvent<Object, Object> record : records) {
            PutObjectRequest putRecord = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(this.map(record.destination(), batchTime))
                    .build();
            LOGGER.debug("Uploading s3File bucket:{} key:{} endpint:{}", putRecord.bucket(), putRecord.key(), endpointOverride);
            s3client.putObject(putRecord, RequestBody.fromBytes(getBytes(record.value())));
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }
}
