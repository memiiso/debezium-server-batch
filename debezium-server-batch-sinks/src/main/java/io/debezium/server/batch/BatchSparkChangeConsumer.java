/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.engine.ChangeEvent;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import com.fasterxml.jackson.databind.JsonNode;
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

  @PostConstruct
  void connect() throws URISyntaxException, InterruptedException {
    this.initizalize();
  }

  @PreDestroy
  void close() {
    this.stopSparkSession();
  }

  @Override
  public long uploadDestination(String destination, List<ChangeEvent<Object, Object>> data) throws InterruptedException {
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
          LOGGER.trace("Spark uploadDestination Json file:{} line val:{}", fileName, line);
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
      df.write()
          .mode(saveMode)
          .format(saveFormat)
          .save(bucket + "/" + uploadFile);

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
          LOGGER.trace("Spark uploadDestination row val:{}", x.toString())
      );
    }
    df.unpersist();

    if (jsonLinesFile.getFile() != null && jsonLinesFile.getFile().exists()) {
      jsonLinesFile.getFile().delete();
    }

    return numRecords;
  }

  public JsonlinesBatchFile getJsonLines(String destination, List<ChangeEvent<Object, Object>> data) {

    Instant start = Instant.now();
    JsonNode valSchema = null;
    JsonNode keySchema = null;
    boolean isFirst = true;
    final File tempFile;
    long numLines = 0L;
    try {
      tempFile = File.createTempFile(UUID.randomUUID() + "-", ".json");
      FileOutputStream fos = new FileOutputStream(tempFile, true);
      LOGGER.debug("Writing {} events as jsonlines file: {}", data.size(), tempFile);

      for (ChangeEvent<Object, Object> e : data) {
        Object val = e.value();
        Object key = e.key();

        if (val == null) {
          LOGGER.warn("Null Value received skipping the entry! destination:{} key:{}", destination, getString(key));
          continue;
        }

        if (isFirst) {
          valSchema = BatchUtil.getJsonSchemaNode(getString(val));
          if (key != null) {
            keySchema = BatchUtil.getJsonSchemaNode(getString(key));
          }
          isFirst = false;
        }

        try {
          final JsonNode valNode = valDeserializer.deserialize(destination, getBytes(val));
          final String valData = mapper.writeValueAsString(valNode) + System.lineSeparator();

          if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Event value Json Node:{}", valNode.toString());
            LOGGER.trace("Event value String:{}", valData);
          }

          fos.write(valData.getBytes(StandardCharsets.UTF_8));
          numLines++;
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

    LOGGER.trace("Writing jsonlines file took:{}",
        Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS));

    // if nothing processed return null
    if (isFirst) {
      tempFile.delete();
      return null;
    }

    return new JsonlinesBatchFile(tempFile, valSchema, keySchema, numLines);
  }

}
