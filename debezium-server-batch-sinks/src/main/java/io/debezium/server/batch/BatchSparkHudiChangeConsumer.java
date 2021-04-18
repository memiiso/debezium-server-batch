/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.UUID;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
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
public class BatchSparkHudiChangeConsumer extends BatchSparkChangeConsumer {

  protected static final String SPARK_HUDI_PROP_PREFIX = "debezium.sink.sparkhudibatch.";
  java.util.Map<String, String> hudioptions = new HashMap<>();
  String saveFormat = "hudi";

  // @TODO table schema
  @ConfigProperty(name = "debezium.sink.sparkhudibatch.table-namespace", defaultValue = "default")
  String namespace;
  @ConfigProperty(name = "debezium.sink.sparkhudibatch.write-operation", defaultValue = "insert")
  String writeOperation;
  @ConfigProperty(name = "debezium.sink.sparkhudibatch.append-recordkey-field", defaultValue = "hudi_uuidpk")
  String appendPkFieldName;
  @ConfigProperty(name = "debezium.sink.sparkhudibatch.precombine-field", defaultValue = "__source_ts_ms")
  String precombineFieldName;

  public void initialize() throws InterruptedException {
    super.initizalize();
    hudioptions = BatchUtil.getConfigSubset(ConfigProvider.getConfig(), SPARK_HUDI_PROP_PREFIX);
  }

  protected void uploadDestination(String destination, JsonlinesBatchFile jsonLinesFile) {

    Instant start = Instant.now();
    // upload different destinations parallel but same destination serial
    if (jsonLinesFile == null) {
      LOGGER.debug("No data to upload for destination: {}", destination);
      return;
    }
    // Read DF with Schema if schema enabled and exists in the event message
    StructType dfSchema = BatchUtil.getSparkDfSchema(jsonLinesFile.getSchema());

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

    String uploadFile = objectStorageNameMapper.map(destination);

    Dataset<Row> df = spark.read().schema(dfSchema).json(jsonLinesFile.getFile().getAbsolutePath());

    // ad PK field for append
    if (writeOperation.equals(WriteOperationType.INSERT.name())) {
      UserDefinedFunction uuid = udf(() -> UUID.randomUUID().toString(), DataTypes.StringType);
      df = df.withColumn(appendPkFieldName, uuid.apply());
    }

    // @TODO add tests
    // @TODO V2 add upsert fallback to append if missing PK key
    // @TODO V2 read table get PK? or extract it from event!?? use it as PK
    String tableName = destination.replace(".", "_");
    String basePath = bucket + "/" + uploadFile;

    // @TODO use table client to read table metadata - PK, and PRECOMBINE_FIELD_PROP
    // HoodieTableMetaClient tclient = new HoodieTableMetaClient.Builder().setConf(null).setBasePath("path").build();

    df.write()
        .options(hudioptions)
        .option(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY, appendPkFieldName)
        .option(HoodieWriteConfig.PRECOMBINE_FIELD_PROP, precombineFieldName)
        .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), writeOperation)
        // @TODO V2 add partitioning hive style by consume time?? __source_ts_ms
        .option(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "")
        .option(HoodieWriteConfig.KEYGENERATOR_CLASS_PROP, NonpartitionedKeyGenerator.class.getCanonicalName())
        .option(HoodieWriteConfig.TABLE_NAME, tableName)
        //.option(TABLE_TYPE_OPT_KEY, HoodieTableType.COPY_ON_WRITE)
        .mode(SaveMode.Append)
        .format(saveFormat)
        .save(basePath);

    LOGGER.info("Uploaded {} rows, schema:{}, file size:{} upload time:{}, saved to:'{}'",
        df.count(),
        dfSchema != null,
        jsonLinesFile.getFile().length(),
        Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS),
        uploadFile);

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

}
