/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.engine.ChangeEvent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("sparkbatchv2")
@Dependent
public class BatchSparkChangeConsumerV2 extends AbstractBatchSparkChangeConsumer {

  @PostConstruct
  void connect() throws InterruptedException {
    this.initizalize();
  }

  @PreDestroy
  void close() {
    this.stopSparkSession();
  }

  public StructType getSparkSchema(ChangeEvent<Object, Object> event) {
    return BatchUtil.getSparkDfSchema(BatchUtil.getJsonSchemaNode(getString(event.value())));
  }

  public Dataset<Row> dataToSparkDf(String destination, List<ChangeEvent<Object, Object>> data) {

    // get spark schema using one event
    StructType dfSchema = getSparkSchema(data.get(0));

    List<String> rowData = new ArrayList<>();
    for (ChangeEvent<Object, Object> event : data) {
      final Object val = event.value();

      if (val == null) {
        LOGGER.warn("Null Value received skipping the entry! destination:{} key:{}", destination, getString(event.key()));
        continue;
      }

      final JsonNode valNode = valDeserializer.deserialize(destination, getBytes(val));

      try {
        rowData.add(mapper.writeValueAsString(valNode));
      } catch (IOException ioe) {
        throw new UncheckedIOException("Failed reading event value", ioe);
      }
    }

    Dataset<String> ds = this.spark.createDataset(rowData, Encoders.STRING());
    Dataset<Row> df;
    if (dfSchema != null) {
      LOGGER.debug("Reading data with schema definition, Schema:\n{}", dfSchema);
      df = spark.read().schema(dfSchema).json(ds);
    } else {
      LOGGER.debug("Reading data without schema definition");
      df = spark.read().json(ds);
    }
    ds.unpersist();
    return df;
  }

  @Override
  public long uploadDestination(String destination, List<ChangeEvent<Object, Object>> data) {

    Instant start = Instant.now();
    Dataset<Row> df = dataToSparkDf(destination, data);
    long numRecords;

    String uploadFile = objectStorageNameMapper.map(destination);
    // serialize same destination uploads
    synchronized (uploadLock.computeIfAbsent(destination, k -> new Object())) {
      df.write()
          .mode(saveMode)
          .format(saveFormat)
          .save(bucket + "/" + uploadFile);

      numRecords = df.count();
      LOGGER.debug("Uploaded {} rows to:'{}' upload time:{}, ",
          numRecords,
          uploadFile,
          Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS)
      );
    }

    if (LOGGER.isTraceEnabled()) {
      df.toJavaRDD().foreach(x ->
          LOGGER.trace("uploadDestination df row val:{}", x.toString())
      );
    }
    df.unpersist();

    return numRecords;
  }

}