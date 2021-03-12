/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.manualtests;


import io.debezium.server.batch.common.TestUtil;

public class testHazelcast extends TestUtil {


  public static int testsize = 100000;

  public static void main(String[] args) {
    testHazelcast mytest = new testHazelcast();
    //mytest.runInfinispan();
    //mytest.runHazelcast();
  }
/*
  public void runHazelcast() {

    NearCacheConfig cfg = new NearCacheConfig();
    HazelcastInstance cm = Hazelcast.newHazelcastInstance();
    ConcurrentMap<String, String> map = cm.getMap("test");
    System.out.println("Using Hazelcast");

    Instant start = Instant.now();
    System.out.println("Putting data Hazelcast");
    IMap<Object, Object> cache = cm.getMap("test");
    for (int i = 0; i < testsize; i++) {
      cache.put(randomString(randomInt(31, 32)), randomString(randomInt(5300, 14300)));
    }

    System.out.println("Cache Size - test: " + cm.getMap("test").size());
    for (Object k : cache.keySet()) {
      cache.remove(k);
    }
    Instant end = Instant.now();
    Duration interval = Duration.between(start, end);
    System.out.println("Cache Size - test: " + cm.getMap("test").size());
    System.out.println("Execution time in seconds hazel: " + interval.getSeconds());
    cm.shutdown();
  }


  public void runInfinispan() {
    ConfigurationBuilder builder = new ConfigurationBuilder();
    DefaultCacheManager cm = new DefaultCacheManager();
    System.out.println("Using simpleCache");
    //builder.simpleCache(true);
    builder
        .persistence()
        .addSingleFileStore()
        .purgeOnStartup(true)
        .location("./cache");
    cm.createCache("test", builder.build());

    Instant start = Instant.now();
    System.out.println("Putting data infini");
    Cache<Object, Object> cache = cm.getCache("test");
    for (int i = 0; i < testsize; i++) {
      cache.put(randomString(randomInt(31, 32)), randomString(randomInt(5300, 14300)));
    }

    System.out.println("Cache Size - test: " + cm.getCache("test").size());
    for (Object k : cache.keySet()) {
      cache.remove(k);
    }
    Instant end = Instant.now();
    Duration interval = Duration.between(start, end);
    System.out.println("Cache Size - test: " + cm.getCache("test").size());
    System.out.println("Execution time in seconds infini: " + interval.getSeconds());
    cm.stop();
  }

  public void runCaffeine() {
    System.out.println("Using caffeine");
    //builder.simpleCache(true);
    com.github.benmanes.caffeine.cache.Cache<Object, Object> cache = Caffeine.newBuilder()
        .build();

    Instant start = Instant.now();
    System.out.println("Putting data infini");
    for (int i = 0; i < testsize; i++) {
      cache.put(randomString(randomInt(31, 32)), randomString(randomInt(5300, 14300)));
    }

    System.out.println("Cache Size - test: " + cache.estimatedSize());
    for (Object k : cache.keySet()) {
      cache.remove(k);
    }
    Instant end = Instant.now();
    Duration interval = Duration.between(start, end);
    System.out.println("Cache Size - test: " + cm.getCache("test").size());
    System.out.println("Execution time in seconds infini: " + interval.getSeconds());
    cm.stop();
  }
  */

}
