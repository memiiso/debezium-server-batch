/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.engine.ChangeEvent;
import io.debezium.server.batch.uploadlock.InterfaceUploadLock;
import io.debezium.server.batch.uploadlock.UploadLockException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("sparkbatch")
@Dependent
public class BatchSparkChangeConsumer extends AbstractBatchSparkChangeConsumer {

  @Inject
  InterfaceUploadLock concurrentUploadLock;

  public void initialize() throws InterruptedException {
    super.initizalize();
    LOGGER.info("Starting Spark Consumer({})", this.getClass().getSimpleName());
    LOGGER.info("Spark save format is '{}'", saveFormat);
  }

  @PostConstruct
  void connect() throws URISyntaxException, InterruptedException {
    this.initizalize();
  }

  @PreDestroy
  void close() {
    this.stopSparkSession();
  }

  @Override
  public long uploadDestination(String destination, ArrayList<ChangeEvent<Object, Object>> data) throws InterruptedException {
    return this.uploadDestination(destination, this.getJsonLines(destination, data));
  }

  protected long uploadDestination(String destination, JsonlinesBatchFile jsonLinesFile) throws InterruptedException {

    Instant start = Instant.now();
    long numRecords = 0L;
    // upload different destinations parallel but same destination serial
    if (jsonLinesFile == null) {
      LOGGER.debug("No data to upload for destination: {}", destination);
      return numRecords;
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
      LOGGER.debug("Reading data with schema definition, Schema:\n{}", dfSchema);
    } else {
      LOGGER.debug("Reading data without schema definition");
    }

    String uploadFile = objectStorageNameMapper.map(destination);

    Dataset<Row> df = spark.read().schema(dfSchema).json(jsonLinesFile.getFile().getAbsolutePath());
    // serialize same destination uploads
    synchronized (uploadLock.computeIfAbsent(destination, k -> new Object())) {
      try (AutoCloseable l = concurrentUploadLock.lock(destination)) {
        df.write()
            .mode(saveMode)
            .format(saveFormat)
            .save(bucket + "/" + uploadFile);
      } catch (Exception e) {
        throw new UploadLockException("Failed to lock! " + e.getMessage());
      }

      numRecords = df.count();
      LOGGER.debug("Uploaded {} rows (read with schema:{}) to:'{}' file:{} file size:{} upload time:{}, ",
          numRecords,
          dfSchema != null,
          uploadFile,
          jsonLinesFile.getFile().getName(),
          jsonLinesFile.getFile().length(),
          Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS)
      );
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

    return numRecords;
  }

}
