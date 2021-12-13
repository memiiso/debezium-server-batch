/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.DebeziumException;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import com.google.cloud.bigquery.*;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("bigquerybatch")
@Dependent
public class BatchBigqueryChangeConsumer extends AbstractBigqueryChangeConsumer {
  @PostConstruct
  void connect() throws InterruptedException {
    this.initizalize();
  }

  @Override
  public long uploadDestination(String destination, List<BatchEvent> data) throws InterruptedException {

    File jsonlines = getJsonLinesFile(destination, data);
    try {
      Instant start = Instant.now();
      long numRecords = Files.lines(jsonlines.toPath()).count();
      TableId tableId = getTableId(destination);

      Schema schema = data.get(0).getBigQuerySchema(castDeletedField);
      Clustering clustering = data.get(0).getBigQueryClustering();

      // serialize same destination uploads
      synchronized (uploadLock.computeIfAbsent(destination, k -> new Object())) {

        WriteChannelConfiguration.Builder wCCBuilder = WriteChannelConfiguration
            .newBuilder(tableId, FormatOptions.json())
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .setClustering(clustering)
            .setTimePartitioning(timePartitioning)
            .setSchemaUpdateOptions(schemaUpdateOptions)
            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED);

        if (schema != null) {
          LOGGER.trace("Setting schema to: {}", schema);
          wCCBuilder.setSchema(schema);
        }
//        else {
//          wCCBuilder.setAutodetect(true);
//        }

        TableDataWriteChannel writer = bqClient.writer(wCCBuilder.build());
        try (OutputStream stream = Channels.newOutputStream(writer)) {
          Files.copy(jsonlines.toPath(), stream);
        }

        Job job = writer.getJob().waitFor();
        if (job.isDone()) {
          LOGGER.debug("Data successfully loaded to {}. {}", tableId, job.getStatistics());
        } else {
          throw new DebeziumException("BigQuery was unable to load into the table due to an error:" + job.getStatus().getError());
        }

      }

      LOGGER.debug("Uploaded {} rows to:{}, upload time:{}, clusteredFields:{}",
          numRecords,
          tableId,
          Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS),
          clustering
      );

      jsonlines.delete();
      return numRecords;

    } catch (BigQueryException | InterruptedException | IOException e) {
      e.printStackTrace();
      throw new DebeziumException(e);
    } finally {
      jsonlines.delete();
    }
  }

}