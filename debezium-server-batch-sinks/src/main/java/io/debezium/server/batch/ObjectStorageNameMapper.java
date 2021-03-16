/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.server.StreamNameMapper;

import java.time.LocalDateTime;
import java.util.Objects;
import javax.enterprise.context.Dependent;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Dependent
public class ObjectStorageNameMapper implements StreamNameMapper {

  @ConfigProperty(name = "debezium.sink.batch.objectkey-partition", defaultValue = "false")
  protected Boolean partitionData;

  @ConfigProperty(name = "debezium.sink.batch.objectkey-prefix", defaultValue = "")
  protected String objectKeyPrefix;

  protected String getPartition() {
    final LocalDateTime batchTime = LocalDateTime.now();
    return "year=" + batchTime.getYear() + "/month=" + StringUtils.leftPad(batchTime.getMonthValue() + "", 2, '0') + "/day="
        + StringUtils.leftPad(batchTime.getDayOfMonth() + "", 2, '0');
  }

  @Override
  public String map(String destination) {
    Objects.requireNonNull(destination, "destination Cannot be Null");
    if (partitionData) {
      String partitioned = getPartition();
      return objectKeyPrefix + destination + "/" + partitioned;
    } else {
      return objectKeyPrefix + destination;
    }
  }
}
