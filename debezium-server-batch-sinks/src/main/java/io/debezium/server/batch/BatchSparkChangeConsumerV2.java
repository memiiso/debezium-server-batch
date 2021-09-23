/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.engine.ChangeEvent;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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