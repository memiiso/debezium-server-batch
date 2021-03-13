/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.manualtests;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;

import io.debezium.server.batch.common.TestUtil;

public class testKeyLength2 extends TestUtil {

  public static void main(String[] args) {
    testKeyLength2 mytest = new testKeyLength2();
    mytest.run();
  }

  public void run() {
    DefaultCacheManager cm = new DefaultCacheManager();
    ConfigurationBuilder builder = new ConfigurationBuilder().simpleCache(true);

    cm.createCache("test2", builder.build());
    ConcurrentHashMap<String, Integer> cacheRowCounter = new ConcurrentHashMap<>();

    Instant start = Instant.now();
    int dummy = 0;
    for (int i = 0; i < 100000; i++) {
      cm.getCache("test2")
          .put(randomString(randomInt(31, 32)), randomString(randomInt(2300, 4300)));
      // dummy = cm.getCache("test2").size()*2;
      cacheRowCounter.put("test2", cacheRowCounter.getOrDefault("test2", 0) + 1);
      dummy = (i + 2) * 2;
    }
    System.out.println("Cache Size:" + dummy);
    Cache<Object, Object> cache = cm.getCache("test2");
    System.out.println("Cache Size:" + cache.size());
    for (Object k : cache.keySet()) {
      Object val = cache.remove(k);
    }
    System.out.println("Cache Size:" + cache.size());

    Instant end = Instant.now();
    Duration interval = Duration.between(start, end);
    System.out.println("Execution time in seconds: " + interval.getSeconds());
  }
}
