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
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;

public class testInfinispan_eviction extends TestUtil {


  public static void main(String[] args) {
    testInfinispan_eviction mytest = new testInfinispan_eviction();
    mytest.run();
  }

  public void run() {
    DefaultCacheManager cm = new DefaultCacheManager();
    ConfigurationBuilder builder = new ConfigurationBuilder();
    if (false) {
      builder.simpleCache(true).statistics().enable();
    }
    builder.persistence()
        // see https://infinispan.org/docs/dev/titles/configuring/configuring.html#passivation_behavior
        // Passivation enabled
        // Infinispan adds data to persistent storage only when it evicts data from memory.
        // Passivation disabled
        // Writes to data in memory result in writes to persistent storage.
        .passivation(false)
        // Local (non-shared) file store
        .addSingleFileStore()
        .preload(true)
        .shared(false)// local cache file, not shared
        .fetchPersistentState(true) // ?
        .ignoreModifications(false) // ??
        .purgeOnStartup(true) // ??
        .location("./cache") // ?
        .async()
        // https://infinispan.org/docs/dev/titles/configuring/configuring.html#async_return_values-configuring
        .enabled(false)// make sure correct value returned
    ;

    builder.persistence()
        .addSingleFileStore()
        .maxEntries(14)
        .location("./cache")
        .statistics().enable();

    ConfigurationBuilder builder2 = new ConfigurationBuilder();
    builder2.memory().maxCount(10)
        .persistence()
        .passivation(true)    // activating passivation
        .addSingleFileStore() // in a single file
        .purgeOnStartup(true) // clean the file on startup
        .location(System.getProperty("java.io.tmpdir"))
        .statistics().enable()
    ;


    cm.createCache("test2", builder.build());
    ConcurrentHashMap<String, Integer> cacheRowCounter = new ConcurrentHashMap<>();

    Instant start = Instant.now();
    for (int i = 0; i < 1000; i++) {
      cm.getCache("test2").put(randomString(randomInt(31, 32)), randomString(randomInt(5300, 14300)));
      cacheRowCounter.put("test2", cacheRowCounter.getOrDefault("test2", 0) + 1);
      //System.out.println("Cache Size: " + cm.getCache("test2").size());
    }

    System.out.println("Stat: " + cm.getCache("test2"));
    System.out.println("Stat: " + cm.getStats().getStores());
    System.out.println("Stat: " + cm.getStats().getCurrentNumberOfEntriesInMemory());
    System.out.println("Stat: " + cm.getStats().getDataMemoryUsed());
    System.out.println("Stat: " + cm.getStats().getOffHeapMemoryUsed());
    System.out.println("Stat: " + cm.getStats().toString());

    Instant end = Instant.now();
    Duration interval = Duration.between(start, end);
    System.out.println("Cache Size: " + cm.getCache("test2").size());
    System.out.println("Execution time in seconds: " + interval.getSeconds());

  }


}
