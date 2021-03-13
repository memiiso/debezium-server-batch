/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.infinispan.cacheperformance;

import static io.debezium.server.batch.common.TestUtil.randomInt;
import static io.debezium.server.batch.common.TestUtil.randomString;

import java.io.IOException;

import org.eclipse.microprofile.config.ConfigProvider;
import org.fest.assertions.Assertions;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.junit.jupiter.api.Test;

import io.debezium.server.batch.cache.BatchJsonlinesFile;
import io.debezium.server.batch.cache.InfinispanCache;
import io.debezium.server.batch.common.TestChangeEvent;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@TestProfile(TestInfinispanCache2TestResource.class)
class TestInfinispanCache2 {
  protected static final Integer rowLimit =
      ConfigProvider.getConfig()
          .getOptionalValue("debezium.sink.batch.row-limit", Integer.class)
          .orElse(AbstractStoreConfiguration.MAX_BATCH_SIZE.getDefaultValue());

  @Test
  void testResetCacheSize() throws IOException {
    String destination = "cachesizetest";
    InfinispanCache mycache = new InfinispanCache();
    Assertions.assertThat(0 == mycache.getEstimatedCacheSize(destination));
    int rownumber = rowLimit * 5;
    for (int i = 0; i < rownumber; i++) {
      final TestChangeEvent<Object, Object> a =
          new TestChangeEvent<>(
              "key",
              "{\"id\": 1, \"first_name\": \"" + randomString(randomInt(1024, 1524)) + "\"}",
              null);
      mycache.append(destination, a);
    }
    for (int i = 0; i < (rownumber / rowLimit); i++) {
      BatchJsonlinesFile jsonlines = mycache.getJsonLines(destination);
      System.out.println("File size : " + jsonlines.getFile().length());
      jsonlines.getFile().delete();
    }
    System.out.println("Final cache size is " + mycache.getEstimatedCacheSize(destination));
  }
}
