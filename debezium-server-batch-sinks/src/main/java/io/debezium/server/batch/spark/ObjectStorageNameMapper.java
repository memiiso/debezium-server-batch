/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *  
 */

package io.debezium.server.batch.spark;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.Optional;
import javax.enterprise.context.Dependent;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Dependent
public class ObjectStorageNameMapper implements StreamNameMapper {

  @ConfigProperty(name = "debezium.sink.batch.objectkey-partition", defaultValue = "false")
  protected Boolean partitionData;

  @ConfigProperty(name = "debezium.sink.batch.objectkey-partition-time-zone", defaultValue = "UTC")
  protected String partitionDataZone;

  @ConfigProperty(name = "debezium.sink.batch.objectkey-prefix", defaultValue = "")
  protected Optional<String> objectKeyPrefix;

  @ConfigProperty(name = "debezium.sink.batch.destination-regexp", defaultValue = "")
  protected Optional<String> destinationRegexp;

  @ConfigProperty(name = "debezium.sink.batch.destination-regexp-replace", defaultValue = "")
  protected Optional<String> destinationRegexpReplace;

  public String getPartition() {
    final ZonedDateTime batchTime = ZonedDateTime.now(ZoneId.of(partitionDataZone));
    return "year=" + batchTime.getYear() + "/month=" + StringUtils.leftPad(batchTime.getMonthValue() + "", 2, '0') + "/day="
        + StringUtils.leftPad(batchTime.getDayOfMonth() + "", 2, '0');
  }

  @Override
  public String map(String destination) {
    Objects.requireNonNull(destination, "destination Cannot be Null");
    if (partitionData) {
      String partitioned = getPartition();
      return objectKeyPrefix.orElse("") +
          destination.replaceAll(destinationRegexp.orElse(""), destinationRegexpReplace.orElse("")) +
          "/" + partitioned;
    } else {
      return objectKeyPrefix + destination.replaceAll(destinationRegexp.orElse(""), destinationRegexpReplace.orElse(""));
    }
  }
}
