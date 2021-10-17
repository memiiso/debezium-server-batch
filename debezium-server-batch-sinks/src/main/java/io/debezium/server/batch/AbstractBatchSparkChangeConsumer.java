/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.engine.ChangeEvent;
import io.debezium.server.batch.streammapper.ObjectStorageNameMapper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public abstract class AbstractBatchSparkChangeConsumer extends AbstractBatchChangeConsumer {

  protected static final String SPARK_PROP_PREFIX = "debezium.sink.sparkbatch.";
  protected static final ConcurrentHashMap<String, Object> uploadLock = new ConcurrentHashMap<>();
  protected final SparkConf sparkconf = new SparkConf()
      .setAppName("CDC-Batch-Spark-Sink")
      .setMaster("local[*]");
  protected SparkSession spark;

  @Inject
  protected ObjectStorageNameMapper streamMapper;
  @ConfigProperty(name = "debezium.sink.sparkbatch.bucket-name", defaultValue = "s3a://My-S3-Bucket")
  String bucket;
  @ConfigProperty(name = "debezium.sink.sparkbatch.save-format", defaultValue = "parquet")
  String saveFormat;
  @ConfigProperty(name = "debezium.sink.sparkbatch.save-mode", defaultValue = "append")
  String saveMode;
  @ConfigProperty(name = "debezium.sink.sparkbatch.cast-deleted-field", defaultValue = "false")
  Boolean castDeletedField;

  protected void stopSparkSession() {
    try {
      LOGGER.info("Closing Spark");
      if (!spark.sparkContext().isStopped()) {
        spark.close();
      }
      LOGGER.debug("Closed Spark");
    } catch (Exception e) {
      LOGGER.warn("Exception during Spark shutdown ", e);
    }
  }

  public void initizalize() throws InterruptedException {
    super.initizalize();

    Map<String, String> appSparkConf = BatchUtil.getConfigSubset(ConfigProvider.getConfig(), SPARK_PROP_PREFIX);
    appSparkConf.forEach(this.sparkconf::set);
    this.sparkconf.set("spark.ui.enabled", "false");

    LOGGER.info("Creating Spark session");
    this.spark = SparkSession
        .builder()
        .config(this.sparkconf)
        .getOrCreate();

    LOGGER.info("Starting Spark Consumer({})", this.getClass().getSimpleName());
    LOGGER.info("Spark save format is '{}'", saveFormat);
    LOGGER.info("Spark Version {}", this.spark.version());
    LOGGER.info("Spark Config Values\n{}", this.spark.sparkContext().getConf().toDebugString());
  }

  protected StructType getSparkSchema(ChangeEvent<Object, Object> event) {
    return BatchUtil.getSparkDfSchema(BatchUtil.getJsonSchemaNode(getString(event.value())));
  }

  public File getJsonLinesFile(String destination, List<ChangeEvent<Object, Object>> data) {

    Instant start = Instant.now();
    final File tempFile;
    try {
      tempFile = File.createTempFile(UUID.randomUUID() + "-", ".json");
      FileOutputStream fos = new FileOutputStream(tempFile, true);
      LOGGER.debug("Writing {} events as jsonlines file: {}", data.size(), tempFile);

      for (ChangeEvent<Object, Object> e : data) {
        Object val = e.value();

        if (val == null) {
          LOGGER.warn("Null Value received skipping the entry! destination:{} key:{}", destination, getString(e.key()));
          continue;
        }

        try {
          final JsonNode valNode = valDeserializer.deserialize(destination, getBytes(val));
          final String valData = mapper.writeValueAsString(valNode) + System.lineSeparator();

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

    LOGGER.trace("Writing jsonlines took:{}", Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS));
    return tempFile;
  }


//  protected Dataset<Row> dataToSparkDf(String destination, List<ChangeEvent<Object, Object>> data) {
//
//    // get spark schema using one event
//    StructType dfSchema = getSparkSchema(data.get(0));
//    File jsonlines = getJsonLinesFile(destination, data);
//
////    List<String> rowData = new ArrayList<>();
////    for (ChangeEvent<Object, Object> event : data) {
////      final Object val = event.value();
////
////      if (val == null) {
////        LOGGER.warn("Null Value received skipping the entry! destination:{} key:{}", destination, getString(event.key()));
////        continue;
////      }
////
////      final JsonNode valNode = valDeserializer.deserialize(destination, getBytes(val));
////
////      try {
////        rowData.add(mapper.writeValueAsString(valNode));
////      } catch (IOException ioe) {
////        throw new UncheckedIOException("Failed reading event value", ioe);
////      }
////    }
////    Dataset<String> ds = this.spark.createDataset(rowData, Encoders.STRING());
//
//    Dataset<Row> df;
//    if (dfSchema != null) {
//      LOGGER.debug("Reading data with schema definition, Schema:\n{}", dfSchema);
//      df = spark.read().schema(dfSchema).json(jsonlines.getAbsolutePath());
//    } else {
//      LOGGER.debug("Reading data without schema definition");
//      df = spark.read().json(jsonlines.getAbsolutePath());
//    }
////    ds.unpersist();
//
//    if (jsonlines.exists()) {
//      jsonlines.delete();
//    }
//
//    return df;
//  }

}
