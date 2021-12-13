/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *  
 */

package io.debezium.server.batch.spark;

import java.util.Optional;
import javax.enterprise.context.Dependent;

import org.eclipse.microprofile.config.inject.ConfigProperty;

@Dependent
public class DestinationMapperBigquery implements DestinationMapper {

  @ConfigProperty(name = "debezium.sink.batch.destination-regexp", defaultValue = "")
  protected Optional<String> destinationRegexp;
  @ConfigProperty(name = "debezium.sink.batch.destination-regexp-replace", defaultValue = "")
  protected Optional<String> destinationRegexpReplace;
  @ConfigProperty(name = "debezium.sink.sparkbatch.spark.datasource.bigquery.dataset", defaultValue = "")
  Optional<String> bqDataset;

  @Override
  public void initialize() throws InterruptedException {
    if (bqDataset.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.sparkbatch.spark.datasource.bigquery.dataset`");
    }
  }

  @Override
  public String map(String destination) {
    return bqDataset.get() + "." +
        destination
            .replaceAll(destinationRegexp.orElse(""), destinationRegexpReplace.orElse(""))
            .replace(".", "_");
  }
}
