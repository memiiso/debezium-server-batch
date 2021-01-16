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

import java.io.Closeable;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("icebergevents")
@Dependent
public class IcebergEventsChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  static final String TABLE_NAME = "debezium_events";

  // @TODO add schema enabled flags! for key and value!
  // @TODO add flattened flag SMT unwrap! as bolean?
  // @TODO extract value from key and store only value -> event_key_value!
  // @TODO add event_sink_timestamp to partition
  static final Schema TABLE_SCHEMA = new Schema(
      required(1, "event_destination", Types.StringType.get(), "event destination"),
      optional(2, "event_key", Types.StringType.get()),
      optional(3, "event_key_value", Types.StringType.get()),
      optional(4, "event_value", Types.StringType.get()),
      optional(5, "event_sink_timestamp", Types.TimestampType.withZone()));
  static final PartitionSpec TABLE_PARTITION = PartitionSpec.builderFor(TABLE_SCHEMA).identity("event_destination").build();
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergEventsChangeConsumer.class);
  private static final String PROP_PREFIX = "debezium.sink.iceberg.";
  @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
  String valueFormat;
  @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
  String keyFormat;
  Configuration hadoopConf = new Configuration();
  @ConfigProperty(name = PROP_PREFIX + "catalog-impl" /* CatalogProperties.CATALOG_IMPL */, defaultValue = "hadoop")
  String catalogImpl;
  @ConfigProperty(name = PROP_PREFIX + "warehouse" /* CatalogProperties.WAREHOUSE_LOCATION */, defaultValue = "iceberg_warehouse")
  String warehouseLocation;
  @ConfigProperty(name = PROP_PREFIX + "fs.defaultFS")
  String defaultFs;

  Catalog icebergCatalog;
  Table eventTable;

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

    if (!icebergCatalog.tableExists(TableIdentifier.of(TABLE_NAME))) {
      icebergCatalog.createTable(TableIdentifier.of(TABLE_NAME), TABLE_SCHEMA, TABLE_PARTITION);
    }
    eventTable = icebergCatalog.loadTable(TableIdentifier.of(TABLE_NAME));
    // hadoopTables = new HadoopTables(hadoopConf);// do we need this ??
    // @TODO iceberg 11 . make catalog dynamic using catalogImpl CatalogUtil!
    // if (catalogImpl != null) {
    // icebergCatalog = CatalogUtil.loadCatalog(catalogImpl, name, options, hadoopConf);
    // }

  }

  // @PreDestroy
  // void close() {
  // // try {
  // // s3client.close();
  // // }
  // // catch (Exception e) {
  // // LOGGER.error("Exception while closing S3 client: ", e);
  // // }
  // }

  public GenericRecord getIcebergRecord(String destination, ChangeEvent<Object, Object> record, LocalDateTime batchTime) {
    Map<String, Object> var1 = Maps.newHashMapWithExpectedSize(TABLE_SCHEMA.columns().size());
    var1.put("event_destination", destination);
    var1.put("event_key", getString(record.key()));
    var1.put("event_key_value", null); // @TODO extract key value!
    var1.put("event_value", getString(record.value()));
    var1.put("event_sink_timestamp", batchTime.atOffset(ZoneOffset.UTC));
    return GenericRecord.create(TABLE_SCHEMA).copy(var1);
  }

  public String map(String destination) {
    return destination.replace(".", "-");
  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {
    LocalDateTime batchTime = LocalDateTime.now(ZoneOffset.UTC);

    Map<String, ArrayList<ChangeEvent<Object, Object>>> result = records.stream()
        .collect(Collectors.groupingBy(
            objectObjectChangeEvent -> map(objectObjectChangeEvent.destination()),
            Collectors.mapping(p -> p,
                Collectors.toCollection(ArrayList::new))));

    for (Map.Entry<String, ArrayList<ChangeEvent<Object, Object>>> destEvents : result.entrySet()) {
      // each destEvents is set of events for a single table
      ArrayList<Record> destIcebergRecords = destEvents.getValue().stream()
          .map(e -> getIcebergRecord(destEvents.getKey(), e, batchTime))
          .collect(Collectors.toCollection(ArrayList::new));

      commitBatch(destEvents.getKey(), destIcebergRecords, batchTime);
    }
    // committer.markProcessed(record);
    committer.markBatchFinished();
  }

  private void commitBatch(String destination, ArrayList<Record> icebergRecords, LocalDateTime batchTime) throws InterruptedException {
    final String fileName = UUID.randomUUID() + "-" + batchTime + "." + FileFormat.PARQUET.toString().toLowerCase();
    // NOTE! manually setting partition directory here to destination
    OutputFile out = eventTable.io().newOutputFile(eventTable.locationProvider().newDataLocation("event_destination=" + destination + "/" + fileName));

    FileAppender<Record> writer;
    try {
      writer = Parquet.write(out)
          .createWriterFunc(GenericParquetWriter::buildWriter)
          .forTable(eventTable)
          .overwrite()
          .build();

      try (Closeable toClose = writer) {
        writer.addAll(icebergRecords);
      }

    } catch (IOException e) {
      LOGGER.error("Failed committing events to iceberg table!", e);
      throw new InterruptedException(e.getMessage());
    }

    PartitionKey pk = new PartitionKey(TABLE_PARTITION, TABLE_SCHEMA);
    Record pr = GenericRecord.create(TABLE_SCHEMA).copy("event_destination", destination, "event_sink_timestamp", batchTime);
    pk.partition(pr);

    DataFile dataFile = DataFiles.builder(eventTable.spec())
        .withFormat(FileFormat.PARQUET)
        .withPath(out.location())
        .withFileSizeInBytes(writer.length())
        .withSplitOffsets(writer.splitOffsets())
        .withMetrics(writer.metrics())
        .withPartition(pk)
        .build();

    LOGGER.debug("Appending new file '{}' !", dataFile.path());
    eventTable.newAppend()
        .appendFile(dataFile)
        .commit();
    LOGGER.info("Committed events to table! {}", eventTable.location());
  }

}
