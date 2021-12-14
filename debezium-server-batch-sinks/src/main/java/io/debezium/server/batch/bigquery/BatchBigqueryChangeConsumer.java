/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.bigquery;

import io.debezium.DebeziumException;
import io.debezium.server.batch.AbstractChangeConsumer;
import io.debezium.server.batch.BatchEvent;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("bigquerybatch")
@Dependent
public class BatchBigqueryChangeConsumer extends AbstractChangeConsumer {

  @ConfigProperty(name = "debezium.sink.batch.destination-regexp", defaultValue = "")
  protected Optional<String> destinationRegexp;
  @ConfigProperty(name = "debezium.sink.batch.destination-regexp-replace", defaultValue = "")
  protected Optional<String> destinationRegexpReplace;
  @Inject
  @ConfigProperty(name = "debezium.sink.bigquerybatch.dataset", defaultValue = "")
  Optional<String> bqDataset;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.location", defaultValue = "US")
  String bqLocation;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.project", defaultValue = "")
  Optional<String> gcpProject;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.createDisposition", defaultValue = "CREATE_IF_NEEDED")
  String createDisposition;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.partitionField", defaultValue = "__source_ts")
  String partitionField;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.partitionType", defaultValue = "MONTH")
  String partitionType;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.allowFieldAddition", defaultValue = "true")
  Boolean allowFieldAddition;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.allowFieldRelaxation", defaultValue = "true")
  Boolean allowFieldRelaxation;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.credentialsFile", defaultValue = "")
  Optional<String> credentialsFile;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.cast-deleted-field", defaultValue = "false")
  Boolean castDeletedField;

  BigQuery bqClient;
  TimePartitioning timePartitioning;
  List<JobInfo.SchemaUpdateOption> schemaUpdateOptions = new ArrayList<>();

  @PostConstruct
  void connect() throws InterruptedException {
    this.initizalize();
  }

  public void initizalize() throws InterruptedException {

    if (gcpProject.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.bigquerybatch.project`");
    }

    if (bqDataset.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.bigquerybatch.dataset`");
    }

    if (credentialsFile.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.bigquerybatch.credentialsFile`");
    }

    GoogleCredentials credentials;
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsFile.get())) {
      credentials = GoogleCredentials.fromStream(serviceAccountStream);
    } catch (IOException e) {
      throw new DebeziumException(e);
    }

    bqClient = BigQueryOptions.newBuilder()
        .setCredentials(credentials)
        .setProjectId(gcpProject.get())
        .setLocation(bqLocation)
        .build()
        .getService();

    timePartitioning =
        TimePartitioning.newBuilder(TimePartitioning.Type.valueOf(partitionType)).setField(partitionField).build();

    if (allowFieldAddition) {
      schemaUpdateOptions.add(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION);
    }
    if (allowFieldRelaxation) {
      schemaUpdateOptions.add(JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION);
    }

    super.initizalize();
  }


  @Override
  public long uploadDestination(String destination, List<BatchEvent> data) throws InterruptedException {

    File jsonlines = getJsonLinesFile(destination, data);
    try {
      Instant start = Instant.now();
      long numRecords = Files.lines(jsonlines.toPath()).count();
      TableId tableId = getTableId(destination);

      Schema schema = data.get(0).getBigQuerySchema(castDeletedField);
      Clustering clustering = data.get(0).getBigQueryClustering();

      // serialize same destination uploads
      synchronized (uploadLock.computeIfAbsent(destination, k -> new Object())) {

        WriteChannelConfiguration.Builder wCCBuilder = WriteChannelConfiguration
            .newBuilder(tableId, FormatOptions.json())
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .setClustering(clustering)
            .setTimePartitioning(timePartitioning)
            .setSchemaUpdateOptions(schemaUpdateOptions)
            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED);

        if (schema != null) {
          LOGGER.trace("Setting schema to: {}", schema);
          wCCBuilder.setSchema(schema);
        }
//        else {
//          wCCBuilder.setAutodetect(true);
//        }

        TableDataWriteChannel writer = bqClient.writer(wCCBuilder.build());
        try (OutputStream stream = Channels.newOutputStream(writer)) {
          Files.copy(jsonlines.toPath(), stream);
        }

        Job job = writer.getJob().waitFor();
        if (job.isDone()) {
          LOGGER.debug("Data successfully loaded to {}. {}", tableId, job.getStatistics());
        } else {
          throw new DebeziumException("BigQuery was unable to load into the table due to an error:" + job.getStatus().getError());
        }

      }

      LOGGER.debug("Uploaded {} rows to:{}, upload time:{}, clusteredFields:{}",
          numRecords,
          tableId,
          Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS),
          clustering
      );

      jsonlines.delete();
      return numRecords;

    } catch (BigQueryException | InterruptedException | IOException e) {
      e.printStackTrace();
      throw new DebeziumException(e);
    } finally {
      jsonlines.delete();
    }
  }

  TableId getTableId(String destination) {
    final String tableName = destination
        .replaceAll(destinationRegexp.orElse(""), destinationRegexpReplace.orElse(""))
        .replace(".", "_");
    return TableId.of(gcpProject.get(), bqDataset.get(), tableName);
  }

  @Override
  public JsonNode getPayload(String destination, Object val) {
    JsonNode pl = valDeserializer.deserialize(destination, getBytes(val));
    // used to partition tables __source_ts
    ((ObjectNode) pl).put("__source_ts", pl.get("__source_ts_ms").longValue() / 1000);
    return pl;
  }


}