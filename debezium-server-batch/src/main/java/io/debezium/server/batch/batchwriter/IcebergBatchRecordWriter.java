/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.batch.batchwriter;

import io.debezium.server.batch.keymapper.ObjectKeyMapper;

import java.net.URISyntaxException;
import java.time.LocalDateTime;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public class IcebergBatchRecordWriter extends AbstractBatchRecordWriter {
  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergBatchRecordWriter.class);
  private static final String PROP_PREFIX = "debezium.sink.batch.";

  Configuration hadoopConf = new Configuration();
  String catalogImpl = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.catalog-impl", String.class).orElse("hadoop");
  String warehouseLocation = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.warehouse", String.class).orElse("/iceberg/warehouse");
  String defaultFs = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.fs.defaultFS", String.class).orElse("");
  Catalog icebergCatalog;

  public IcebergBatchRecordWriter(ObjectKeyMapper mapper) throws URISyntaxException {
    super(mapper);

    // loop and set hadoopConf
    for (String name : ConfigProvider.getConfig().getPropertyNames()) {
      if (name.startsWith(PROP_PREFIX)) {
        this.hadoopConf.set(name.substring(PROP_PREFIX.length()), ConfigProvider.getConfig().getValue(name, String.class));
        LOGGER.debug("Setting Hadoop Conf '{}' from application.properties!", name.substring(PROP_PREFIX.length()));
      }
    }

    if (warehouseLocation == null || warehouseLocation.trim().isEmpty()) {
      warehouseLocation = defaultFs + "/iceberg/warehouse";
    }
    icebergCatalog = new HadoopCatalog("iceberg", hadoopConf, warehouseLocation);

    LOGGER.info("Starting Iceberg BatchRecordWriter({})", this.getClass().getName());
  }

  //
  // @ProcessElement
  // public void processElement(@Element String word, OutputReceiver<GenericRecord> out) {
  // GenericRecord record = new GenericData.Record(schema);
  // record.put("word", word);
  // out.output(record);
  // }
  //
  // @Test
  // public void testWriteFiles() {
  // PipelineOptions options = PipelineOptionsFactory.create();
  // Pipeline p = Pipeline.create(options);
  // String hiveMetastoreUrl = "thrift://localhost:9083/default";
  //
  // Schema schema = new Schema.Parser().parse(stringSchema);
  // p.getCoderRegistry().registerCoderForClass(GenericRecord.class, AvroCoder.of(schema));
  //
  // PCollection<String> lines = p.apply(TextIO.read().from(getInputFile("words.txt"))).setCoder(StringUtf8Coder.of());
  //
  // PCollection<GenericRecord> records = lines.apply(ParDo.of(new StringToGenericRecord()));
  //
  // FileIO.Write<Void, GenericRecord> fileIO = FileIO.<GenericRecord>write()
  // .via(AvroIO.sink(schema))
  // .to("/tmp/fokko/")
  // .withSuffix(".avro");
  //
  // WriteFilesResult<Void> output = records.apply(fileIO);
  // org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(schema);
  // TableIdentifier name = TableIdentifier.of("default", "test");
  //
  // IcebergIO.write(name, icebergSchema, hiveMetastoreUrl, output);
  //
  // p.run();
  // }

  private void getDataFile() {
    LocalDateTime batchTime = LocalDateTime.now();
    // ArrayList<Record> icebergRecords = Lists.newArrayList();
    // GenericRecord icebergRecord = GenericRecord.create(TABLE_SCHEMA);
    //
    // for (ChangeEvent<Object, Object> record : records) {
    // Map<String, Object> var1 = Maps.newHashMapWithExpectedSize(TABLE_SCHEMA.columns().size());
    // var1.put("event_destination", record.destination());
    // var1.put("event_key", getString(record.key()));
    // var1.put("event_key_value", null); // @TODO extract key value!
    // var1.put("event_value", getString(record.value()));
    // var1.put("event_value_format", valueFormat);
    // var1.put("event_key_format", keyFormat);
    // var1.put("event_sink_timestamp", LocalDateTime.now().atOffset(ZoneOffset.UTC));
    // icebergRecords.add(icebergRecord.copy(var1));
    // }
    //
    // final String fileName = UUID.randomUUID() + "-" + batchTime.toEpochSecond(ZoneOffset.UTC) + "-" + batchId + "." + FileFormat.PARQUET.toString().toLowerCase();
    // OutputFile out = eventTable.io().newOutputFile(eventTable.locationProvider().newDataLocation(fileName));
    //
    // FileAppender<Record> writer = null;
    // try {
    // writer = Parquet.write(out)
    // .createWriterFunc(GenericParquetWriter::buildWriter)
    // .forTable(eventTable)
    // .overwrite()
    // .build();
    //
    // try (Closeable toClose = writer) {
    // writer.addAll(icebergRecords);
    // }
    //
    // }
    // catch (IOException e) {
    // LOGGER.error("Failed committing events to iceberg table!", e);
    // throw new InterruptedException(e.getMessage());
    // }
    //
    // DataFile dataFile = DataFiles.builder(eventTable.spec())
    // .withFormat(FileFormat.PARQUET)
    // .withPath(out.location())
    // .withFileSizeInBytes(writer.length())
    // .withSplitOffsets(writer.splitOffsets())
    // .withMetrics(writer.metrics())
    // .build();
    //
    // LOGGER.debug("Appending new file '{}' !", dataFile.path());
    // eventTable.newAppend()
    // .appendFile(dataFile)
    // .commit();
    // LOGGER.info("Committed events to table! {}", eventTable.location());

  }

  protected void uploadBatchFile(String destination) {
    // @TODO process records line by line loop
    // @TODO convert line to json -> then genericrecord!
    // @TODO get table from catalog!
    Integer batchId = map_batchid.get(destination);
    final String data = map_data.get(destination);
    TableIdentifier tIdentifier = TableIdentifier.of(destination);
    String s3File = objectKeyMapper.map(destination, batchTime, batchId, "iceberg");
    LOGGER.debug("Commiting Iceberg Table :'{}' key:'{}'", destination, s3File);
    // List<String> jsonData = Arrays.asList(data.split(IOUtils.LINE_SEPARATOR));
    // DataFrameReader dfReader = spark.read();
    // // Read DF with Schema if schema exists
    // if (!jsonData.isEmpty()) {
    // this.setReaderSchema(dfReader, Iterables.getLast(jsonData));
    // Dataset<Row> df = dfReader.json(ds);
    // if (removeSchema && Arrays.asList(df.columns()).contains("payload")) {
    // df = df.select("payload.*");
    // }
    // try {
    // df.write().format(saveFormat).mode("append").save(fileName);
    // }
    // catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
    // HadoopTables tables = new HadoopTables(this.spark.sparkContext().hadoopConfiguration());
    // tables.create(SparkSchemaUtil.convert(df.schema()), fileName);
    // LOGGER.debug("Created Table:{}", fileName);
    // df.write().format(saveFormat).mode("append").save(fileName);
    // }
    //
    // }
    // // increment batch id
    // map_batchid.put(destination, batchId + 1);
    // // start new batch
    // map_data.remove(destination);
    // cdcDb.commit();
    // LOGGER.debug("Upload Succeeded! destination:'{}' key:'{}'", destination, fileName);
  }

}
