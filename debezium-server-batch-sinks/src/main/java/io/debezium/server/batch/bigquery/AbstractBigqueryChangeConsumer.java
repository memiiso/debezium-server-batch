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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;

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
public abstract class AbstractBigqueryChangeConsumer extends AbstractChangeConsumer {

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

  public TableId getTableId(String destination) {
    final String tableName = destination
        .replaceAll(destinationRegexp.orElse(""), destinationRegexpReplace.orElse(""))
        .replace(".", "_");
    return TableId.of(gcpProject.get(), bqDataset.get(), tableName);
  }

  @Override
  public JsonNode getPayload(String destination, Object val) {
    JsonNode pl = valDeserializer.deserialize(destination, getBytes(val));
    ((ObjectNode) pl).put("__source_ts", pl.get("__source_ts_ms").longValue() / 1000);
    return pl;
  }

}