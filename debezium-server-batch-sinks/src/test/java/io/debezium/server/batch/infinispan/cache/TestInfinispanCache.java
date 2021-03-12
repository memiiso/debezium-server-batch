/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.infinispan.cache;

import io.debezium.engine.ChangeEvent;
import io.debezium.server.batch.cache.BatchJsonlinesFile;
import io.debezium.server.batch.cache.InfinispanCache;
import io.debezium.server.batch.common.TestChangeEvent;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.eclipse.microprofile.config.ConfigProvider;
import org.fest.assertions.Assertions;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.junit.jupiter.api.Test;
import static io.debezium.server.batch.common.TestUtil.randomInt;
import static io.debezium.server.batch.common.TestUtil.randomString;


@QuarkusTest
@TestProfile(TestInfinispanCacheTestResource.class)
class TestInfinispanCache {
  protected static final Integer maxBatchSize = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.cache.max-batch-size", Integer.class).orElse(AbstractStoreConfiguration.MAX_BATCH_SIZE.getDefaultValue());

  @Test
  void testGetJsonLines() throws IOException {
    InfinispanCache mycache = new InfinispanCache();
    String destination = "test";
    System.out.println(maxBatchSize);

    Assertions.assertThat(0 == mycache.getEstimatedCacheSize(destination));
    ChangeEvent<Object, Object> a = new TestChangeEvent<>("key", "{\"id\": 1, \"first_name\": \"mytest123Value\"}",
        null);
    mycache.append(destination, a);
    Assertions.assertThat(1 == mycache.getEstimatedCacheSize(destination));

    a = new TestChangeEvent<>("key", "{\"id\": 1, \"first_name\": \"mytest2222Value\"}",
        null);
    mycache.append(destination, a);
    Assertions.assertThat(2 == mycache.getEstimatedCacheSize(destination));

    ArrayList<ChangeEvent<Object, Object>> batchData = new ArrayList<>();
    ChangeEvent<Object, Object> c = new TestChangeEvent<>("key", "{\"id\": 1, \"first_name\": \"mytest333Value\"}",
        null);
    batchData.add(c);
    batchData.add(c);
    batchData.add(c);
    mycache.appendAll(destination, batchData);
    Assertions.assertThat(5 == mycache.getEstimatedCacheSize(destination));

    BatchJsonlinesFile jsonlines = mycache.getJsonLines("test");
    String fileContent = Files.readString(Paths.get(jsonlines.getFile().getAbsolutePath()));

    System.out.println(jsonlines);

    String[] lines = fileContent.split("\r\n|\r|\n");
    Assertions.assertThat(lines[0].contains("mytest123Value"));
    Assertions.assertThat(lines[1].contains("mytest2222Value"));
    Assertions.assertThat(lines[2].contains("mytest333Value"));
    Assertions.assertThat(lines.length == 3);

    Assertions.assertThat(5 - maxBatchSize == mycache.getEstimatedCacheSize(destination));

    System.out.println(jsonlines.getFile().getAbsolutePath());
  }

  @Test
  void testResetCacheSize() throws IOException {
    String destination = "cachesizetest";
    InfinispanCache mycache = new InfinispanCache();
    Assertions.assertThat(0 == mycache.getEstimatedCacheSize(destination));
    int rownumber = 1000;
    for (int i = 0; i < rownumber; i++) {
      final TestChangeEvent<Object, Object> a = new TestChangeEvent<>("key",
          "{\"id\": 1, \"first_name\": \"" + randomString(randomInt(5300, 14300)) + "\"}",
          null);
      mycache.append(destination, a);
    }
    for (int i = 0; i < (rownumber / maxBatchSize); i++) {
      BatchJsonlinesFile jsonlines = mycache.getJsonLines(destination);
      jsonlines.getFile().delete();
    }
    System.out.println("Final cache size is " + mycache.getEstimatedCacheSize(destination));
  }

}