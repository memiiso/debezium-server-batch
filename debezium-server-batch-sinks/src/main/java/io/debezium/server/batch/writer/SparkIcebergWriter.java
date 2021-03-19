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

import java.util.Map;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Alternative;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.types.StructType;
import org.eclipse.microprofile.config.ConfigProvider;

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
  Catalog catalog;

  public SparkIcebergWriter() {
  }


  @Override
  public void initialize() {
    super.initialize();
    LOGGER.info("Starting Spark Iceberg Consumer({})", this.getClass().getName());
    LOGGER.info("Spark save format is '{}'", saveFormat);
    // sparkSessionCatalog = (SparkSessionCatalog) spark.sessionState().catalogManager().v2SessionCatalog();
    Map<String, String> icebergConf = BatchUtil.getConfigSubset(ConfigProvider.getConfig(), "debezium.sink.icebergsparkbatch.");
    LOGGER.error("{}", spark.sparkContext().hadoopConfiguration());
    HadoopTables tables = new HadoopTables(this.spark.sparkContext().hadoopConfiguration());
    LOGGER.error("{}", tables.getConf());
    catalog = CatalogUtil.buildIcebergCatalog("name", icebergConf, spark.sparkContext().hadoopConfiguration());
    LOGGER.error("catalog {}", catalog.name());
  }

  @Override
  public void uploadDestination(String destination, BatchJsonlinesFile jsonLinesFile) {
    String iceberg_database = "default";
    String iceberg_table = destination.replace(".", "_");

    try {
      Table table = sparkSessionCatalog.loadTable(Identifier.of(new String[]{iceberg_database}, iceberg_table));
      LOGGER.error("Spark table found, {}", table.name());
      LOGGER.error("Table schema, {}", table.schema());
    } catch (NoSuchTableException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
      throw new RuntimeException("Table not found!", e);
    }

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
      LOGGER.error("Appending to table!");
      df.writeTo(iceberg_database + "." + iceberg_table).append();
    } catch (NoSuchTableException e) {
      try {
        LOGGER.error("Table not found creating it!");
        df.writeTo(iceberg_database + "." + iceberg_table).using("iceberg").create();
      } catch (TableAlreadyExistsException e2) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted in call to rename", e2);
      }
    }
    df.unpersist();
    LOGGER.info("Saved data to:'{}' rows:{}", destination, df.count());
  }

}
