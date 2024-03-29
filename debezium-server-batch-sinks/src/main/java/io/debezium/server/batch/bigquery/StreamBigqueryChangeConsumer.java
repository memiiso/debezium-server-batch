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
import io.debezium.server.batch.DebeziumEvent;
import io.grpc.Status;
import io.grpc.Status.Code;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Implementation of the consumer that delivers the messages to Bigquery
 *
 * @author Ismail Simsek
 */
@Named("bigquerystream")
@Dependent
public class StreamBigqueryChangeConsumer extends AbstractChangeConsumer {
  protected static final ConcurrentHashMap<String, DataWriter> jsonStreamWriters = new ConcurrentHashMap<>();
  @ConfigProperty(name = "debezium.sink.batch.destination-regexp", defaultValue = "")
  protected Optional<String> destinationRegexp;
  @ConfigProperty(name = "debezium.sink.batch.destination-regexp-replace", defaultValue = "")
  protected Optional<String> destinationRegexpReplace;
  @Inject
  @ConfigProperty(name = "debezium.sink.bigquerystream.dataset", defaultValue = "")
  Optional<String> bqDataset;
  @ConfigProperty(name = "debezium.sink.bigquerystream.project", defaultValue = "")
  Optional<String> gcpProject;
  @ConfigProperty(name = "debezium.sink.bigquerystream.location", defaultValue = "US")
  String bqLocation;
  @ConfigProperty(name = "debezium.sink.bigquerystream.cast-deleted-field", defaultValue = "false")
  Boolean castDeletedField;
  @ConfigProperty(name = "debezium.sink.bigquerystream.ignoreUnknownFields", defaultValue = "true")
  Boolean ignoreUnknownFields;
  @ConfigProperty(name = "debezium.sink.bigquerystream.createIfNeeded", defaultValue = "true")
  Boolean createIfNeeded;
  @ConfigProperty(name = "debezium.sink.bigquerystream.partitionField", defaultValue = "__source_ts")
  String partitionField;
  @ConfigProperty(name = "debezium.sink.bigquerystream.partitionType", defaultValue = "MONTH")
  String partitionType;
  @ConfigProperty(name = "debezium.sink.bigquerystream.allowFieldAddition", defaultValue = "false")
  Boolean allowFieldAddition;
  @ConfigProperty(name = "debezium.sink.bigquerystream.credentialsFile", defaultValue = "")
  Optional<String> credentialsFile;

  TimePartitioning timePartitioning;
  private static final int MAX_RETRY_COUNT = 2;
  private static final ImmutableList<Code> RETRIABLE_ERROR_CODES =
      ImmutableList.of(Code.INTERNAL, Code.ABORTED, Code.CANCELLED);
  public static BigQueryWriteClient bigQueryWriteClient;
  BigQuery bqClient;

  @PostConstruct
  void connect() throws InterruptedException {
    this.initizalize();
  }

  @PreDestroy
  void closeStreams() {
    for (Map.Entry<String, DataWriter> sw : jsonStreamWriters.entrySet()) {
      try {
        sw.getValue().close();
      } catch (Exception e) {
        e.printStackTrace();
        LOGGER.warn("Exception while closing bigquery stream, destination:" + sw.getKey(), e);
      }
    }
  }


  public void initizalize() throws InterruptedException {
    super.initizalize();

    if (gcpProject.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.bigquerystream.project`");
    }

    if (bqDataset.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.bigquerystream.dataset`");
    }

    timePartitioning =
        TimePartitioning.newBuilder(TimePartitioning.Type.valueOf(partitionType)).setField(partitionField).build();

    GoogleCredentials credentials;
    try {
      if (credentialsFile.isPresent()) {
        credentials = GoogleCredentials.fromStream(new FileInputStream(credentialsFile.get()));
      } else {
        credentials = GoogleCredentials.getApplicationDefault();
      }
    } catch (IOException e) {
      throw new DebeziumException("Failed to initialize google credentials", e);
    }

    bqClient = BigQueryOptions.newBuilder()
        .setCredentials(credentials)
        .setProjectId(gcpProject.get())
        .setLocation(bqLocation)
        .setRetrySettings(
            RetrySettings.newBuilder()
                // Set the max number of attempts
                .setMaxAttempts(5)
                // InitialRetryDelay controls the delay before the first retry. 
                // Subsequent retries will use this value adjusted according to the RetryDelayMultiplier. 
                .setInitialRetryDelay(org.threeten.bp.Duration.ofSeconds(5))
                .setMaxRetryDelay(org.threeten.bp.Duration.ofSeconds(60))
                // Set the backoff multiplier
                .setRetryDelayMultiplier(2.0)
                // Set the max duration of all attempts
                .setTotalTimeout(org.threeten.bp.Duration.ofMinutes(5))
                .build()
        )
        .build()
        .getService();

    try {
      BigQueryWriteSettings bigQueryWriteSettings = BigQueryWriteSettings
          .newBuilder()
          .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
          .build();
      bigQueryWriteClient = BigQueryWriteClient.create(bigQueryWriteSettings);
    } catch (IOException e) {
      throw new DebeziumException("Failed to create BigQuery Write Client", e);
    }
  }

  private DataWriter getDataWriter(Table table) {
    try {
      Schema schema = table.getDefinition().getSchema();
      TableSchema tableSchema = DebeziumBigqueryEvent.convertBigQuerySchema2TableSchema(schema);
      return new DataWriter(
          TableName.of(table.getTableId().getProject(), table.getTableId().getDataset(), table.getTableId().getTable()),
          tableSchema, ignoreUnknownFields);
    } catch (DescriptorValidationException | IOException | InterruptedException e) {
      throw new DebeziumException("Failed to initialize stream writer for table " + table.getTableId(), e);
    }
  }

  private static JSONObject jsonNode2JSONObject(JsonNode jsonNode, Boolean castDeletedField) {
    Map<String, Object> jsonMap = mapper.convertValue(jsonNode, new TypeReference<>() {
    });
    if (castDeletedField && jsonMap.containsKey("__deleted")) {
      jsonMap.replace("__deleted", Boolean.parseBoolean((String) jsonMap.get("__deleted")));
    }
    return new JSONObject(jsonMap);
  }

  @Override
  public long uploadDestination(String destination, List<DebeziumEvent> data) {
    long numRecords = data.size();
    Table table = getTable(destination, data.get(0));
    DataWriter writer = jsonStreamWriters.computeIfAbsent(destination, k -> getDataWriter(table));
    try {
      JSONArray jsonArr = new JSONArray();
      data.forEach(e -> jsonArr.put(jsonNode2JSONObject(e.value(), castDeletedField)));
      writer.appendSync(jsonArr);
    } catch (DescriptorValidationException | IOException e) {
      throw new DebeziumException("Failed to append data to stream " + writer.streamWriter.getStreamName(), e);
    }
    LOGGER.debug("Appended {} records to {} successfully.", numRecords, destination);
    return numRecords;
  }

  public TableId getTableId(String destination) {
    final String tableName = destination
        .replaceAll(destinationRegexp.orElse(""), destinationRegexpReplace.orElse(""))
        .replace(".", "_");
    return TableId.of(gcpProject.get(), bqDataset.get(), tableName);
  }

  private Table getTable(String destination, DebeziumEvent sampleEvent) {
    TableId tableId = getTableId(destination);
    Table table = bqClient.getTable(tableId);
    // create table if missing
    if (createIfNeeded && table == null) {
      DebeziumBigqueryEvent sampleBqEvent = new DebeziumBigqueryEvent(sampleEvent);
      Schema schema = sampleBqEvent.getBigQuerySchema(castDeletedField, true);
      Clustering clustering = sampleBqEvent.getBigQueryClustering();

      StandardTableDefinition tableDefinition =
          StandardTableDefinition.newBuilder()
              .setSchema(schema)
              .setTimePartitioning(timePartitioning)
              .setClustering(clustering)
              .build();
      TableInfo tableInfo =
          TableInfo.newBuilder(tableId, tableDefinition).build();
      table = bqClient.create(tableInfo);
    }

    if (allowFieldAddition) {
      DebeziumBigqueryEvent sampleBqEvent = new DebeziumBigqueryEvent(sampleEvent);
      Schema eventSchema = sampleBqEvent.getBigQuerySchema(castDeletedField, true);

      List<Field> tableFields = new ArrayList<>(table.getDefinition().getSchema().getFields());
      List<String> fieldNames = tableFields.stream().map(Field::getName).collect(Collectors.toList());

      boolean fieldAddition = false;
      for (Field field : eventSchema.getFields()) {
        if (!fieldNames.contains(field.getName())) {
          tableFields.add(field);
          fieldAddition = true;
        }
      }

      if (fieldAddition) {
        LOGGER.debug("Updating table with the new fields");
        Schema newSchema = Schema.of(tableFields);
        Table updatedTable = table.toBuilder().setDefinition(StandardTableDefinition.of(newSchema)).build();
        table = updatedTable.update();
        jsonStreamWriters.get(destination).close();
        jsonStreamWriters.replace(destination, getDataWriter(table));
        LOGGER.info("New columns successfully added to table");
      }
    }
    return table;
  }

  protected static class DataWriter {
    private final JsonStreamWriter streamWriter;

    public DataWriter(TableName parentTable, TableSchema tableSchema, Boolean ignoreUnknownFields)
        throws DescriptorValidationException, IOException, InterruptedException {
      // Use the JSON stream writer to send records in JSON format. Specify the table name to write
      // to the default stream. For more information about JsonStreamWriter, see:
      // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1/JsonStreamWriter.html
      streamWriter =
          JsonStreamWriter
              .newBuilder(parentTable.toString(), tableSchema, bigQueryWriteClient)
              .setIgnoreUnknownFields(ignoreUnknownFields)
              .build();
    }

    private void appendSync(JSONArray data, int retryCount) throws DescriptorValidationException,
        IOException {
      ApiFuture<AppendRowsResponse> future = streamWriter.append(data);
      try {
        AppendRowsResponse response = future.get();
        if (response.hasError()) {
          throw new DebeziumException("Failed to append data to stream. " + response.getError().getMessage());
        }
      } catch (InterruptedException | ExecutionException throwable) {
        // If the wrapped exception is a StatusRuntimeException, check the state of the operation.
        // If the state is INTERNAL, CANCELLED, or ABORTED, you can retry. For more information,
        // see: https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html
        Status status = Status.fromThrowable(throwable);
        if (retryCount < MAX_RETRY_COUNT
            && RETRIABLE_ERROR_CODES.contains(status.getCode())) {
          try {
            // Since default stream appends are not ordered, we can simply retry the appends.
            // Retrying with exclusive streams requires more careful consideration.
            this.appendSync(data, ++retryCount);
            // Mark the existing attempt as done since it's being retried.
          } catch (Exception e) {
            throw new DebeziumException("Failed to append data to stream", e);
          }
        }
      }

    }

    public void appendSync(JSONArray data)
        throws DescriptorValidationException, IOException {
      this.appendSync(data, 0);
    }

    public void close() {
      if (streamWriter != null) {
        streamWriter.close();
      }
    }
  }

}

