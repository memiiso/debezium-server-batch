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
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.manager.DefaultCacheManager;

import io.debezium.server.batch.common.TestUtil;

public class testInfinispanpersistent extends TestUtil {

  public static void main(String[] args) {
    testInfinispanpersistent mytest = new testInfinispanpersistent();
    mytest.run();
  }

  public void run() {
    ConfigurationBuilder builder = new ConfigurationBuilder();
    GlobalConfiguration globalConfig =
        new GlobalConfigurationBuilder()
            .cacheContainer()
            .statistics(true)
            .metrics()
            .gauges(true)
            .histograms(true)
            .jmx()
            .enable()
            .shutdown()
            .hookBehavior(ShutdownHookBehavior.DONT_REGISTER)
            .build();

    DefaultCacheManager cm = new DefaultCacheManager(globalConfig);

    if (false) {
      builder.simpleCache(true).statistics().enable();
    }
    builder
        .statistics()
        .enable()
        // The cache passivation is one of the powerful features of Infinispan.
        // By combining passivation and eviction, we can create a cache that doesn't occupy a lot of
        // memory,
        // without losing information.
        // https://www.baeldung.com/infinispan#4-passivation-cache
        // .memory().evictionType(EvictionType.COUNT).size(1)
        .memory()
        .maxCount(125)
        // PersistenceConfigurationBuilder
        .persistence()
        // see
        // https://infinispan.org/docs/dev/titles/configuring/configuring.html#passivation_behavior
        // Passivation enabled
        // Infinispan adds data to persistent storage only when it evicts data from memory.
        // Passivation disabled
        // Writes to data in memory result in writes to persistent storage.
        .passivation(false)
        // Adds a single file cache store.. Local (non-shared) file store
        .addSingleFileStore()
        // If true, when the cache starts, data stored in the cache store will be pre-loaded into
        // memory.
        .preload(true)
        // local cache file, not shared
        .shared(false)
        // is this needed ?
        .fetchPersistentState(true)
        // If true, any operation that modifies the cache (put, remove, clear, store...etc) won't be
        // applied to the cache store.
        // This means that the cache store could become out of sync with the cache.
        .ignoreModifications(false)
        // If true, purges this cache store when it starts up.
        .purgeOnStartup(false)
        // Sets a location on disk where the store can write.
        .location("./cache")
        .statistics()
        .enable();

    // init cacheRowCounter if cache is restarted
    System.out.println("{1}" + cm.getCacheConfigurationNames());
    for (String destination : cm.getCacheConfigurationNames()) {
      System.out.println(
          "Loaded-1 {} records from cache for destination:{}"
              + cm.getCache(destination).size()
              + "-- "
              + destination);
    }

    cm.createCache("test", builder.build());
    cm.createCache("test2", builder.build());
    // init cacheRowCounter if cache is restarted
    System.out.println("{2}" + cm.getCacheConfigurationNames());
    for (String destination : cm.getCacheConfigurationNames()) {
      System.out.println(
          "Loaded-2 {} records from cache for destination:{}"
              + cm.getCache(destination).size()
              + "-- "
              + destination);
    }
    ConcurrentHashMap<String, Integer> cacheRowCounter = new ConcurrentHashMap<>();

    Instant start = Instant.now();
    for (int i = 0; i < 1000; i++) {
      cm.getCache("test")
          .put(randomString(randomInt(31, 32)), randomString(randomInt(5300, 14300)));
      cm.getCache("test2").put(randomString(randomInt(31, 32)), randomString(randomInt(11, 22)));
      cacheRowCounter.put("test2", cacheRowCounter.getOrDefault("test2", 0) + 1);
      // System.out.println("Cache Size: " + cm.getCache("test2").size());
    }

    System.out.println("Cache Size: " + cm.getCache("test2").size());
    Cache<Object, Object> cache = cm.getCache("test2");
    int l = 0;
    for (Object k : cache.keySet()) {
      Object val = cache.remove(k);
      System.out.println(val);
      l++;
      if (l > 10) {
        break;
      }
    }

    Object k = cache.keySet().stream().findFirst().get();
    for (int i = 0; i < 11; i++) {
      cache.put(k, i + "-->" + cache.get(k) + "-" + i);
      System.out.println(cache.get(k));
    }

    System.out.println("Cache Size: " + cm.getCache("test2").size());

    System.out.println(
        "Stat:getCurrentNumberOfEntriesInMemory "
            + cm.getStats().getCurrentNumberOfEntriesInMemory());
    System.out.println("Stat:getDataMemoryUsed " + cm.getStats().getDataMemoryUsed());
    System.out.println("Stat:getOffHeapMemoryUsed " + cm.getStats().getOffHeapMemoryUsed());

    Instant end = Instant.now();
    Duration interval = Duration.between(start, end);
    System.out.println("Cache Size: " + cm.getCache("test2").size());
    System.out.println("Cache Size: " + cm.getCache("test").size());
    System.out.println("Execution time in seconds: " + interval.getSeconds());
  }
}
