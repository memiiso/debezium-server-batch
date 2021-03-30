/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.cache;

import io.debezium.engine.ChangeEvent;
import io.debezium.server.batch.JsonlinesBatchFile;
import io.debezium.server.batch.common.TestChangeEvent;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.Test;
import static io.debezium.server.batch.common.TestUtil.randomInt;
import static io.debezium.server.batch.common.TestUtil.randomString;
import static org.junit.jupiter.api.Assertions.assertEquals;


@QuarkusTest
@TestProfile(InfinispanCacheTestProfile.class)
class InfinispanCacheTest {

  @ConfigProperty(name = "debezium.sink.batch.row-limit")
  Integer maxBatchSize;

  @Inject
  InfinispanCache mycache;

  @Test
  void testGetJsonLines() throws IOException {
    mycache.initialize();
    String destination = "test";

    Assertions.assertThat(0 == mycache.getEstimatedCacheSize(destination));
    ChangeEvent<Object, Object> a = new TestChangeEvent<>("key", "{\"id\": 1, \"first_name\": \"mytest123Value\"}",
        null);
    mycache.appendAll(destination, List.of(a));
    Assertions.assertThat(1 == mycache.getEstimatedCacheSize(destination));

    a = new TestChangeEvent<>("key", "{\"id\": 1, \"first_name\": \"mytest2222Value\"}",
        null);
    mycache.appendAll(destination, List.of(a));
    Assertions.assertThat(2 == mycache.getEstimatedCacheSize(destination));

    ArrayList<ChangeEvent<Object, Object>> batchData = new ArrayList<>();
    ChangeEvent<Object, Object> c = new TestChangeEvent<>("key", "{\"id\": 1, \"first_name\": \"mytest333Value\"}",
        null);
    batchData.add(c);
    batchData.add(c);
    batchData.add(c);
    mycache.appendAll(destination, batchData);
    Assertions.assertThat(5 == mycache.getEstimatedCacheSize(destination));

    JsonlinesBatchFile jsonlines = mycache.getJsonLines("test");
    String fileContent = Files.readString(Paths.get(jsonlines.getFile().getAbsolutePath()));

    String[] lines = fileContent.split("\r\n|\r|\n");
    Assertions.assertThat(lines[0].contains("mytest123Value"));
    Assertions.assertThat(lines[1].contains("mytest2222Value"));
    Assertions.assertThat(lines[2].contains("mytest333Value"));
    Assertions.assertThat(lines.length == 3);

    Assertions.assertThat(5 - maxBatchSize == mycache.getEstimatedCacheSize(destination));
  }

  @Test
  void testEstimatedCacheSize() {
    mycache.initialize();
    String destination = "cachesizetest";
    Assertions.assertThat(0 == mycache.getEstimatedCacheSize(destination));
    int rownumber = 10 * maxBatchSize;
    for (int i = 0; i < rownumber; i++) {
      final TestChangeEvent<Object, Object> a = new TestChangeEvent<>("key",
          "{\"id\": 1, \"first_name\": \"" + randomString(randomInt(5300, 14300)) + "\"}",
          null);
      mycache.appendAll(destination, List.of(a));
    }
    assertEquals(10 * maxBatchSize, mycache.getEstimatedCacheSize(destination));

    for (int i = 0; i < (rownumber / maxBatchSize); i++) {
      JsonlinesBatchFile jsonlines = mycache.getJsonLines(destination);
      jsonlines.getFile().delete();
    }
    assertEquals(0, mycache.getEstimatedCacheSize(destination));
  }

}