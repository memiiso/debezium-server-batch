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
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("sparkhudibatch")
@Dependent
public class BatchSparkHudiChangeConsumer extends BatchSparkChangeConsumer {

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
    // serialize same destination uploads
    synchronized (uploadLock.computeIfAbsent(destination, k -> new Object())) {
      // @TODO remove synchronized block, since hudi is consistent
      // @TODO add hudi append, set hudi_uuidpk as PK
      // @TODO add partitioning hive style
      // @TODO add tests
      // @TODO use function to get append df to separate append and upsert
      // @TODO upsert fallback to append if missing key
      df.write()
          .mode(saveMode)
          .format("hudi")
          .save(bucket + "/" + uploadFile);
      LOGGER.info("Uploaded {} rows, schema:{}, file size:{} upload time:{}, saved to:'{}'",
          df.count(),
          dfSchema != null,
          jsonLinesFile.getFile().length(),
          Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS),
          uploadFile);
    }

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
