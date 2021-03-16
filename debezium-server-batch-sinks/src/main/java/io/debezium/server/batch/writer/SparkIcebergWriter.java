/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.writer;

import io.debezium.server.batch.BatchJsonlinesFile;
import io.debezium.server.batch.BatchUtil;

import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Alternative;

import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.types.StructType;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Dependent
@Alternative
public class SparkIcebergWriter extends AbstractSparkWriter {

  final String saveFormat = "iceberg";

  protected SparkSessionCatalog sparkSessionCatalog;

  public SparkIcebergWriter() {
  }

  @Override
  public void initialize() {
    super.initialize();
    LOGGER.info("Starting Spark Iceberg Consumer({})", this.getClass().getName());
    LOGGER.info("Spark save format is '{}'", saveFormat);
  }

  @Override
  public void uploadDestination(String destination, BatchJsonlinesFile jsonLinesFile) {
    //String iceberg_table = map(destination);

    // Read DF with Schema if schema enabled and exists in the event message
    if (jsonLinesFile == null) {
      LOGGER.info("No data received to upload for destination: {}", destination);
      return;
    }

    StructType dfSchema = BatchUtil.getSparkDfSchema(jsonLinesFile.getSchema());

    if (dfSchema != null) {
      LOGGER.info("Reading data with schema");
      LOGGER.debug("Schema:\n{}", jsonLinesFile.getSchema());
    }

    Dataset<Row> df = spark.read().schema(dfSchema).json(jsonLinesFile.getFile().getAbsolutePath());
    try {
      // @TODO add database and create get function to clean table name+return table identifier
      df.writeTo("default." + destination.replace(".", "_")).append();
    } catch (NoSuchTableException e) {
      try {
        df.writeTo("default." + destination.replace(".", "_")).using("iceberg").create();
      } catch (TableAlreadyExistsException e2) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted in call to rename", e2);
      }
    }

    df.unpersist();
    LOGGER.info("Saved data to:'{}' rows:{}", destination, df.count());
  }

}
