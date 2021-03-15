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
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.batch.cache.BatchJsonlinesFile;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("sparkbatch")
@Dependent
public class SparkBatchChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  protected static final Logger LOGGER = LoggerFactory.getLogger(SparkBatchChangeConsumer.class);
  protected static final String SPARK_PROP_PREFIX = "debezium.sink.sparkbatch.";
  protected static final ConcurrentHashMap<String, Object> uploadLock = new ConcurrentHashMap<>();
  protected final SparkConf sparkconf = new SparkConf()
      .setAppName("CDC-Batch-Spark-Sink")
      .setMaster("local[*]");
  protected final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  protected final ObjectMapper mapper = new ObjectMapper();
  @Inject
  protected S3StreamNameMapper s3StreamNameMapper;
  protected Deserializer<JsonNode> valDeserializer;
  @ConfigProperty(name = "debezium.sink.sparkbatch.save-format", defaultValue = "json")
  protected String saveFormat;
  @ConfigProperty(name = "debezium.sink.sparkbatch.bucket-name", defaultValue = "My-S3-Bucket")
  protected String bucket;
  @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
  String valueFormat;
  @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
  String keyFormat;
  private SparkSession spark;

  @PreDestroy
  void close() {
    try {
      LOGGER.info("Closing spark!");
      spark.close();
    } catch (Exception e) {
      LOGGER.warn("Exception while closing spark:{} ", e.getMessage());
      e.printStackTrace();
    }
  }

  @PostConstruct
  void connect() throws URISyntaxException, InterruptedException {

    LOGGER.info("Using '{}' batch writer", this.getClass().getName());
    LOGGER.info("Using bucket: '{}'", bucket);

    valSerde.configure(Collections.emptyMap(), false);
    valDeserializer = valSerde.deserializer();

    for (String name : ConfigProvider.getConfig().getPropertyNames()) {
      if (name.startsWith(SPARK_PROP_PREFIX)
          && !name.contains("secret") && !name.contains("password") && !name.contains("acess.key")) {
        this.sparkconf.set(name.substring(SPARK_PROP_PREFIX.length()), ConfigProvider.getConfig().getValue(name, String.class));
      }
    }
    this.sparkconf.set("spark.ui.enabled", "false");

    LOGGER.info("Creating Spark session");
    this.spark = SparkSession
        .builder()
        .config(this.sparkconf)
        .getOrCreate();

    LOGGER.info("Spark Config Values\n{}", this.spark.sparkContext().getConf().toDebugString());

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

    Map<String, ArrayList<ChangeEvent<Object, Object>>> result = records.stream()
        .collect(Collectors.groupingBy(
            ChangeEvent::destination,
            Collectors.mapping(p -> p,
                Collectors.toCollection(ArrayList::new))));

    for (Map.Entry<String, ArrayList<ChangeEvent<Object, Object>>> destinationEvents : result.entrySet()) {
      this.uploadDestination(destinationEvents.getKey(), destinationEvents.getValue());
    }
    // workaround! somehow offset is not saved to file unless we call committer.markProcessed
    // even its should be saved to file periodically
    if (!records.isEmpty()) {
      committer.markProcessed(records.get(0));
    }
    committer.markBatchFinished();
  }

  public BatchJsonlinesFile getJsonLines(String destination, ArrayList<ChangeEvent<Object, Object>> data) {

    JsonNode schema = null;
    boolean isFirst = true;
    final File tempFile;
    try {
      tempFile = File.createTempFile(UUID.randomUUID() + "-", ".json");
      FileOutputStream fos = new FileOutputStream(tempFile, true);

      for (ChangeEvent<Object, Object> e : data) {
        Object val = e.value();

        // this could happen if multiple threads reading and removing data
        if (val == null) {
          LOGGER.warn("Cache.getJsonLines Null Event Value found for destination:'{}'! " +
              "skipping the entry!", destination);
          continue;
        }
        LOGGER.trace("Cache.getJsonLines val:{}", getString(val));

        if (isFirst) {
          schema = BatchUtil.getJsonSchemaNode(getString(val));
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

    // if nothing processed return null
    if (isFirst) {
      tempFile.delete();
      return null;
    }

    return new BatchJsonlinesFile(tempFile, schema);
  }

  public void uploadDestination(String destination, ArrayList<ChangeEvent<Object, Object>> data) {
    Instant start = Instant.now();
    // upload different destinations parallel but same destination serial
    BatchJsonlinesFile tempFile = this.getJsonLines(destination, data);
    if (tempFile == null) {
      LOGGER.debug("No data to upload for destination: {}", destination);
      return;
    }
    // Read DF with Schema if schema enabled and exists in the event message
    StructType dfSchema = BatchUtil.getSparkDfSchema(tempFile.getSchema());

    if (LOGGER.isTraceEnabled()) {
      final String fileName = tempFile.getFile().getName();
      try (BufferedReader br = new BufferedReader(new FileReader(tempFile.getFile().getAbsolutePath()))) {
        String line;
        while ((line = br.readLine()) != null) {
          LOGGER.trace("SparkConsumer.uploadDestination Json file:{} line val:{}", fileName, line);
        }
      } catch (Exception e) {
        LOGGER.warn("Exception happened during debug logging!", e);
      }
    }

    if (dfSchema != null) {
      LOGGER.debug("Reading data with schema definition. Schema:\n{}", dfSchema);
    } else {
      LOGGER.debug("Reading data without schema definition");
    }

    String s3File = s3StreamNameMapper.map(destination);

    Dataset<Row> df = spark.read().schema(dfSchema).json(tempFile.getFile().getAbsolutePath());
    // serialize same destination uploads
    synchronized (uploadLock.computeIfAbsent(destination, k -> new Object())) {
      df.write()
          .mode(SaveMode.Append)
          .format(saveFormat)
          .save(bucket + "/" + s3File);
      LOGGER.info("Uploaded {} rows, schema:{}, file size:{} upload time:{}, saved to:'{}'",
          df.count(),
          dfSchema != null,
          tempFile.getFile().length(),
          Duration.between(start, Instant.now()),
          s3File);
    }

    if (LOGGER.isTraceEnabled()) {
      df.toJavaRDD().foreach(x ->
          LOGGER.trace("SparkConsumer.uploadDestination row val:{}", x.toString())
      );
    }
    df.unpersist();

    if (tempFile.getFile() != null && tempFile.getFile().exists()) {
      tempFile.getFile().delete();
    }
  }

}
