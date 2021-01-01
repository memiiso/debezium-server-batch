/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.batch;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import org.apache.kafka.connect.json.JsonDeserializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.batch.batchwriter.BatchRecordWriter;
import io.debezium.server.batch.batchwriter.S3JsonBatchRecordWriter;
import io.debezium.server.batch.batchwriter.SparkBatchRecordWriter;
import io.debezium.server.batch.batchwriter.SparkDeltaBatchRecordWriter;
import io.debezium.server.batch.batchwriter.SparkIcebergBatchRecordWriter;
import io.debezium.server.batch.keymapper.DefaultObjectKeyMapper;
import io.debezium.server.batch.keymapper.LakeTableObjectKeyMapper;
import io.debezium.server.batch.keymapper.ObjectKeyMapper;
import io.debezium.server.batch.keymapper.TimeBasedDailyObjectKeyMapper;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("batch")
@Dependent
public class BatchChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(BatchChangeConsumer.class);
    final String valueFormat = ConfigProvider.getConfig().getOptionalValue("debezium.format.value", String.class).orElse(Json.class.getSimpleName().toLowerCase());

    ObjectKeyMapper keyMapper;
    BatchRecordWriter batchWriter;

    @Inject
    @ConfigProperty(name = "debezium.sink.batch.objectkey.mapper")
    String customKeyMapper;

    @Inject
    @ConfigProperty(name = "debezium.sink.batch.batchwriter")
    String customBatchWriter;

    JsonDeserializer jsonDeserializer = new JsonDeserializer();

    @PreDestroy
    void close() {
        try {
            batchWriter.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing batchWriter:{} ", e.getMessage());
        }
    }

    @PostConstruct
    void connect() throws URISyntaxException, InterruptedException {
        switch (customKeyMapper) {
            case "dailypartitioned":
                keyMapper = new TimeBasedDailyObjectKeyMapper();
                break;
            case "table":
                keyMapper = new LakeTableObjectKeyMapper();
                break;
            case "default":
                keyMapper = new DefaultObjectKeyMapper();
                break;
            default:
                throw new InterruptedException("Message here!");
        }
        LOGGER.info("Using '{}' object name mapper", keyMapper.getClass().getName());

        switch (customBatchWriter) {
            case "spark":
                batchWriter = new SparkBatchRecordWriter(keyMapper);
                break;
            case "sparkdelta":
                batchWriter = new SparkDeltaBatchRecordWriter(keyMapper);
                break;
            case "sparkiceberg":
                batchWriter = new SparkIcebergBatchRecordWriter(keyMapper);
                break;
            case "s3json":
                batchWriter = new S3JsonBatchRecordWriter(keyMapper);
                break;
            default:
                throw new InterruptedException("Message here!");
        }

        LOGGER.info("Using '{}' batch writer", batchWriter.getClass().getName());

        if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
            throw new InterruptedException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) value formats are {json,}!");
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        try {
            for (ChangeEvent<Object, Object> record : records) {
                JsonNode valueJson = jsonDeserializer.deserialize(record.destination(), getBytes(record.value()));
                batchWriter.append(record.destination(), valueJson);
                // committer.markProcessed(record);
            }
            batchWriter.uploadBatch();
            committer.markBatchFinished();
        }
        catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            LOGGER.error(sw.toString());
            throw new InterruptedException(e.getMessage());
        }
    }

}
