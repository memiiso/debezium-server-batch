/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.batch.writer.AbstractBatchWriter;
import io.debezium.server.batch.writer.S3JsonWriter;
import io.debezium.server.batch.writer.SparkBatchWriter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("batch")
@Dependent
public class BatchChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  protected static final Logger LOGGER = LoggerFactory.getLogger(BatchChangeConsumer.class);
  @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
  String valueFormat;
  @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
  String keyFormat;
  AbstractBatchWriter batchWriter;
  @Inject
  @ConfigProperty(name = "debezium.sink.batch.writer")
  String customBatchWriter;

  @PreDestroy
  void close() {
    try {
      LOGGER.warn("Close called, uploading all cache data and closing batch writer!");
      batchWriter.uploadAll();
      batchWriter.close();
    } catch (Exception e) {
      LOGGER.warn("Exception while closing writer:{} ", e.getMessage());
    }
  }

  @PostConstruct
  void connect() throws URISyntaxException, InterruptedException {

    switch (customBatchWriter) {
      case "spark":
        batchWriter = new SparkBatchWriter();
        break;
      case "sparkiceberg":
        // @TODO
        batchWriter = new SparkBatchWriter();
        break;
      case "s3json":
        batchWriter = new S3JsonWriter();
        break;
      default:
        throw new InterruptedException("Message here!");
    }

    LOGGER.info("Using '{}' batch writer", batchWriter.getClass().getName());

    if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) formats are {json,}!");
    }
    if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.key={" + valueFormat + "} not supported! Supported (debezium.format.key=*) formats are {json,}!");
    }
  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {
    try {
      for (ChangeEvent<Object, Object> record : records) {
        batchWriter.append(record);
        // committer.markProcessed(record);
      }
      // batchWriter.uploadAll();
      committer.markBatchFinished();
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      LOGGER.error(sw.toString());
      throw new InterruptedException(e.getMessage());
    }
  }

}
