/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *  
 */

package io.debezium.server.batch.spark;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(ObjectStorageNameMapperTestProfile.class)
class ObjectStorageNameMapperTest {

  @Inject
  ObjectStorageNameMapper mapper;

  @ConfigProperty(name = "debezium.sink.batch.objectkey-partition-time-zone", defaultValue = "UTC")
  String partitionDataZone;
  @ConfigProperty(name = "debezium.sink.batch.objectkey-prefix", defaultValue = "XXX")
  String prefix;

  @Test
  void getPartition() {

    final ZonedDateTime batchTime = ZonedDateTime.now(ZoneId.of(partitionDataZone));
    Assertions.assertEquals(mapper.getPartition(),
        "year=" + batchTime.getYear() +
            "/month=" + StringUtils.leftPad(batchTime.getMonthValue() + "", 2, '0') +
            "/day=" + StringUtils.leftPad(batchTime.getDayOfMonth() + "", 2, '0'));
  }

  @Test
  void map() {
    Assertions.assertTrue(
        mapper.map("server822.desti12345nationdb1.table0")
            .startsWith(prefix + "server.destinationdb.table")
    );

    Assertions.assertEquals("MyString", "MyString".replaceAll("", ""));
  }
}