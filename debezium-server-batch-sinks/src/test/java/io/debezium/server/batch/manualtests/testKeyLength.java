/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.manualtests;


import io.debezium.server.batch.common.TestUtil;

import java.time.Duration;
import java.time.Instant;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;

public class testKeyLength extends TestUtil {


  public static void main(String[] args) {
    testKeyLength mytest = new testKeyLength();
    mytest.run();
  }

  public void run() {
    DefaultCacheManager cm = new DefaultCacheManager();
    ConfigurationBuilder builder = new ConfigurationBuilder().simpleCache(true);

    cm.createCache("test", builder.build());

    Instant start = Instant.now();
    for (int i = 0; i < 100000; i++) {
      cm.getCache("test").put(randomString(randomInt(555, 1555)), randomString(randomInt(2300, 4300)));
    }
    Cache<Object, Object> cache = cm.getCache("test");
    System.out.println("Cache Size:" + cache.size());
    for (Object k : cache.keySet()) {
      Object val = cache.remove(k);
    }

    Instant end = Instant.now();
    Duration interval = Duration.between(start, end);
    System.out.println("Execution time in seconds: " + interval.getSeconds());

  }


}
