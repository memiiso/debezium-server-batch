/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.engine.ChangeEvent;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import static org.apache.spark.sql.functions.udf;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("sparkhudibatch")
@Dependent
public class BatchSparkHudiChangeConsumer extends AbstractBatchSparkChangeConsumer {

  protected static final String SPARK_HUDI_PROP_PREFIX = "debezium.sink.sparkhudibatch.";
  @ConfigProperty(name = "debezium.sink.batch.objectkey-prefix", defaultValue = "")
  protected String objectKeyPrefix;
  java.util.Map<String, String> hudioptions = new HashMap<>();
  String saveFormat = "hudi";
  @ConfigProperty(name = "debezium.sink.sparkhudibatch.table-database", defaultValue = "default")
  String database;
  @ConfigProperty(name = "debezium.sink.sparkhudibatch.hoodie.datasource.write.operation", defaultValue = "insert")
  String writeOperation;
  @ConfigProperty(name = "debezium.sink.sparkhudibatch.hoodie.datasource.write.recordkey.field", defaultValue = "hudi_uuidpk")
  String appendRecordKeyFieldName;
  @ConfigProperty(name = "debezium.sink.sparkhudibatch.hoodie.datasource.write.precombine.field", defaultValue = "__source_ts_ms")
  String precombineFieldName;
  @ConfigProperty(name = "debezium.sink.sparkhudibatch.hoodie.datasource.write.table.type", defaultValue = "MERGE_ON_READ")
  String tableType;
  @ConfigProperty(name = "debezium.sink.sparkhudibatch.hoodie.embed.timeline.server", defaultValue = "false")
  Boolean embedTimelineServer;


  public void initialize() throws InterruptedException {
    super.initizalize();
    hudioptions = BatchUtil.getConfigSubset(ConfigProvider.getConfig(), SPARK_HUDI_PROP_PREFIX);
    LOGGER.info("Hudi write mode is:{} record key field(for appends):{}, precombine field: {}", writeOperation,
        appendRecordKeyFieldName,
        precombineFieldName);
  }

  @PostConstruct
  void connect() throws URISyntaxException, InterruptedException {
    this.initizalize();
  }

  @PreDestroy
  void close() {
    this.stopSparkSession();
  }

  protected void uploadDestination(String destination, JsonlinesBatchFile jsonLinesFile) {

    Instant start = Instant.now();
    // upload different destinations parallel but same destination serial
    if (jsonLinesFile == null) {
      LOGGER.debug("No data to upload for destination: {}", destination);
      return;
    }
    // Read DF with Schema if schema enabled and exists in the event message
    StructType dfSchema = BatchUtil.getSparkDfSchema(jsonLinesFile.getValSchema());

    if (LOGGER.isTraceEnabled()) {
      final String fileName = jsonLinesFile.getFile().getName();
      try (BufferedReader br = new BufferedReader(new FileReader(jsonLinesFile.getFile().getAbsolutePath()))) {
        String line;
        while ((line = br.readLine()) != null) {
          LOGGER.trace("SparkWriter.uploadDestination Json file:{} line val:{}", fileName, line);
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

    Dataset<Row> df = spark.read().schema(dfSchema).json(jsonLinesFile.getFile().getAbsolutePath());

    String tablePath = getTablePath(destination);
    String tableName = getTableName(destination);
    final String tableWriteOperation;
    final String tableRecordKeyFieldName;
    final boolean filterDupes;
    StructType dfKeySchema = BatchUtil.getSparkDfSchema(jsonLinesFile.getKeySchema());
    if (writeOperation.equals(WriteOperationType.UPSERT.name())
        // data has key with schema
        && dfKeySchema != null
        // number of key fields is 1, composite keys not supported by hudi
        && dfKeySchema.fields().length == 1
    ) {
      // upsert mode
      tableWriteOperation = writeOperation;
      tableRecordKeyFieldName = dfKeySchema.fields()[0].name();
      filterDupes = true;
      // @TODO use the key as sort order
      LOGGER.debug("Using field {} as record key, filtering duplicated using field {}", tableRecordKeyFieldName, precombineFieldName);
    } else {
      // fallback to append when table don't have PK
      if (writeOperation.equals(WriteOperationType.INSERT.name()) && !(dfKeySchema != null && dfKeySchema.fields().length == 1)) {
        LOGGER.warn("Table {} don't have record key(PK), falling back to append mode", tableName);
      }
      UserDefinedFunction uuid = udf(() -> UUID.randomUUID().toString(), DataTypes.StringType);
      df = df.withColumn(appendRecordKeyFieldName, uuid.apply());
      tableWriteOperation = WriteOperationType.INSERT.name();
      tableRecordKeyFieldName = appendRecordKeyFieldName;
      filterDupes = false;
    }

    LOGGER.error("============================COMMITITNG======" + tableName + "=========================================");
    LOGGER.error("=====DataSourceWriteOptions.TABLE_TYPE_OPT_KEY()====" + DataSourceWriteOptions.TABLE_TYPE_OPT_KEY());
    LOGGER.error("=====DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL()====" + DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL());
    // @TODO V2 add partitioning hive style, by consume time?? __source_ts_ms??
    //.option(TABLE_TYPE_OPT_KEY, HoodieTableType.COPY_ON_WRITE)
    DataFrameWriter<Row> dfhudi = df.write()
        .options(hudioptions)
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), tableRecordKeyFieldName)
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), precombineFieldName)
        .option(DataSourceWriteOptions.INSERT_DROP_DUPS_OPT_KEY(), filterDupes)
        .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), tableWriteOperation)
        // @TODO V2 add partitioning hive style, by consume time?? __source_ts_ms??
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "")
        .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(), NonpartitionedKeyGenerator.class.getCanonicalName())
        .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), tableType)
        .option(HoodieWriteConfig.TABLE_NAME, tableName)
        .option(HoodieWriteConfig.BASE_PATH_PROP, tablePath)
        .option(HoodieWriteConfig.EMBEDDED_TIMELINE_SERVER_ENABLED, embedTimelineServer)
        .option("path", tablePath);

    dfhudi.mode(SaveMode.Append)
        .format(saveFormat)
        .save(tablePath);

    LOGGER.info("Uploaded {} rows, schema:{}, file size:{} upload time:{}, saved to:'{}'",
        df.count(),
        dfSchema != null,
        jsonLinesFile.getFile().length(),
        Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS),
        tablePath);

    if (LOGGER.isTraceEnabled()) {
      df.toJavaRDD().foreach(x ->
          LOGGER.trace("SparkWriter.uploadDestination row val:{}", x.toString())
      );
    }
    df.unpersist();

    if (jsonLinesFile.getFile() != null && jsonLinesFile.getFile().exists()) {
      jsonLinesFile.getFile().delete();
    }
  }

  public String getTableName(String destination) {
    return (objectKeyPrefix + destination).replace(".", "_").replace("-", "_");
  }

  public String getBasePath() {
    return bucket + "/" + database;
  }

  public String getTablePath(String destination) {
    return getBasePath() + "/" + getTableName(destination);
  }

  @Override
  public void uploadDestination(String destination, ArrayList<ChangeEvent<Object, Object>> data) throws InterruptedException {
    this.uploadDestination(destination, this.getJsonLines(destination, data));
  }
}
