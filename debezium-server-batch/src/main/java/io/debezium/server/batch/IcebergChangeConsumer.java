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

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("iceberg")
@Dependent
public class IcebergChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeConsumer.class);
  private static final String PROP_PREFIX = "debezium.sink.iceberg.";
  final Integer batchLimit = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.row.limit", Integer.class).orElse(500);
  @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
  String valueFormat;
  @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
  String keyFormat;
  Configuration hadoopConf = new Configuration();
  @ConfigProperty(name = PROP_PREFIX + "catalog-impl" /* CatalogProperties.CATALOG_IMPL */, defaultValue = "hadoop")
  String catalogImpl;
  @ConfigProperty(name = PROP_PREFIX + "warehouse" /* CatalogProperties.WAREHOUSE_LOCATION */)
  String warehouseLocation;
  @ConfigProperty(name = PROP_PREFIX + "fs.defaultFS")
  String defaultFs;

  Catalog icebergCatalog;
  Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  Deserializer<JsonNode> valDeserializer;

  @PostConstruct
  void connect() throws InterruptedException {
    if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) formats are {json,}!");
    }
    if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.key={" + valueFormat + "} not supported! Supported (debezium.format.key=*) formats are {json,}!");
    }

    // loop and set hadoopConf
    for (String name : ConfigProvider.getConfig().getPropertyNames()) {
      if (name.startsWith(PROP_PREFIX)) {
        this.hadoopConf.set(name.substring(PROP_PREFIX.length()), ConfigProvider.getConfig().getValue(name, String.class));
        LOGGER.debug("Setting Hadoop Conf '{}' from application.properties!", name.substring(PROP_PREFIX.length()));
      }
    }

    if (warehouseLocation == null || warehouseLocation.trim().isEmpty()) {
      warehouseLocation = defaultFs + "/iceberg_warehouse";
    }

    icebergCatalog = new HadoopCatalog("iceberg", hadoopConf, warehouseLocation);
    // @TODO iceberg 11 . make catalog dynamic using catalogImpl parametter!
    // if (catalogImpl != null) {
    // icebergCatalog = CatalogUtil.loadCatalog(catalogImpl, name, options, hadoopConf);
    // }
    valSerde.configure(Collections.emptyMap(), false);
    valDeserializer = valSerde.deserializer();
  }

  public String map(String destination) {
    return destination.replace(".", "-");
  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {

    Map<String, ArrayList<ChangeEvent<Object, Object>>> result = records.stream()
        .collect(Collectors.groupingBy(
            objectObjectChangeEvent -> map(objectObjectChangeEvent.destination()),
            Collectors.mapping(p -> p,
                Collectors.toCollection(ArrayList::new))));

    for (Map.Entry<String, ArrayList<ChangeEvent<Object, Object>>> event : result.entrySet()) {
      Table icebergTable;
      final Schema tableSchema;
      try {
        // load iceberg table
        icebergTable = icebergCatalog.loadTable(TableIdentifier.of(event.getKey()));
      } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
        // Table is not exists lets try to create it using the schema of an debezium event
        JsonNode jsonSchema = null;
        try {
          jsonSchema = new ObjectMapper().readTree(getBytes(event.getValue().get(0).value()));
        } catch (IOException ioException) {
          // pass
        }
        if (BatchUtil.hasSchema(jsonSchema)) {
          Schema schema = BatchUtil.getIcebergSchema(jsonSchema.get("schema"));
          LOGGER.warn("Table '{}' not found creating it!\nSchema:\n{}", TableIdentifier.of(event.getKey()), schema.toString());
          icebergTable = icebergCatalog.createTable(TableIdentifier.of(event.getKey()), schema);
        } else {
          // iceberg table not exists and schema is not enabled to create table with! FAIL!
          e.printStackTrace();
          throw new InterruptedException("Iceberg table not found!\n" + e.getMessage());
        }
      }
      tableSchema = icebergTable.schema();
      ArrayList<Record> icebergRecords = new ArrayList<>();
      for (ChangeEvent<Object, Object> e : event.getValue()) {
        GenericRecord icebergRecord = BatchUtil.getIcebergRecord(tableSchema, valDeserializer.deserialize(e.destination(),
            getBytes(e.value())));
        icebergRecords.add(icebergRecord);
      }

      appendTable(icebergTable, icebergRecords);
    }
    committer.markBatchFinished();
  }

  private void appendTable(Table icebergTable, ArrayList<Record> icebergRecords) throws InterruptedException {
    final String fileName = UUID.randomUUID() + "-" + Instant.now().toEpochMilli() + "." + FileFormat.PARQUET.toString().toLowerCase();
    OutputFile out = icebergTable.io().newOutputFile(icebergTable.locationProvider().newDataLocation(fileName));

    FileAppender<Record> writer;
    try {
      LOGGER.debug("Writing data to file: {}!", out);
      writer = Parquet.write(out)
          .createWriterFunc(GenericParquetWriter::buildWriter)
          .forTable(icebergTable)
          .overwrite()
          .build();

      try (Closeable toClose = writer) {
        writer.addAll(icebergRecords);
      }
    } catch (IOException e) {
      throw new InterruptedException(e.getMessage());
    }

    LOGGER.debug("Building DataFile!");
    DataFile dataFile = DataFiles.builder(icebergTable.spec())
        .withFormat(FileFormat.PARQUET)
        .withPath(out.location())
        .withFileSizeInBytes(writer.length())
        .withSplitOffsets(writer.splitOffsets())
        .withMetrics(writer.metrics())
        .build();

    LOGGER.debug("Committing new file as newAppend '{}' !", dataFile.path());
    icebergTable.newAppend()
        .appendFile(dataFile)
        .commit();
    LOGGER.info("Committed events to table! {}", icebergTable.location());
  }

}
