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

public class testInfinispan extends TestUtil {


  public static void main(String[] args) {
    testInfinispan mytest = new testInfinispan();
    mytest.run();
  }

  public void run() {
    ConfigurationBuilder builder = new ConfigurationBuilder();
    DefaultCacheManager cm = new DefaultCacheManager();
    System.out.println("Using simpleCache");
    builder.simpleCache(true);

    Instant start = Instant.now();
    for (int i = 0; i < 100013; i++) {
      cm.getCache("test").put(randomString(randomInt(31, 32)), randomString(randomInt(5300, 14300)));
    }

    System.out.println("Cache Size - test: " + cm.getCache("test").size());
    Cache<Object, Object> cache = cm.getCache("test");
    for (Object k : cache.keySet()) {
      cache.remove(k);
    }
    Instant end = Instant.now();
    Duration interval = Duration.between(start, end);
    System.out.println("Cache Size - test: " + cm.getCache("test").size());
    System.out.println("Execution time in seconds: " + interval.getSeconds());
  }


}
