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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.codec.digest.DigestUtils;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;

import io.debezium.server.batch.common.TestUtil;

public class testDataRemoval extends TestUtil {

  public static void main(String[] args) {
    testDataRemoval mytest = new testDataRemoval();
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
    int rownumber = 100013;

    boolean is_simple = false;
    boolean is_rocksdb = false;

    if (is_simple) {
      System.out.println("Using simpleCache");
      builder.simpleCache(true).statistics().enable();
    } else if (is_rocksdb) {
      System.out.println("Using Rocksdb");
      Properties props = new Properties();
      props.put("database.max_background_compactions", "12");
      // props.put("data.write_buffer_size", "512MB");
      builder
          .statistics()
          .enable()
          .unsafe()
          .unreliableReturnValues(false)
          // PersistenceConfigurationBuilder
          .persistence()
          .addStore(RocksDBStoreConfigurationBuilder.class)
          .location("rocksdb/data")
          .expiredLocation("rocksdb/expired")
          .properties(props)
          .shared(false)
          // If true, any operation that modifies the cache (put, remove, clear, store...etc) won't
          // be applied to the cache store.
          // This means that the cache store could become out of sync with the cache.
          .ignoreModifications(false)
          // If true, purges this cache store when it starts up.
          .purgeOnStartup(true)
          // Batching
          .invocationBatching()
          .enable(true);
    } else {
      System.out.println("Using Local Cache");
      // 339, enabled: 345, 355
      builder
          .persistence()
          .addSingleFileStore()
          .purgeOnStartup(true)
          .location("./cache")
          .invocationBatching()
          .enable(true);
    }
    // rocksdb 31,32,35
    // local 27,27,27
    // simple local

    Cache<Object, Object> cache1 = cm.createCache("test", builder.build());
    Cache<Object, Object> cache2 = cm.createCache("test2", builder.build());
    Cache<Object, Object> cache3 = cm.createCache("test3", builder.build());
    // init cacheRowCounter if cache is restarted
    System.out.println("{2}" + cm.getCacheConfigurationNames());
    for (String destination : cm.getCacheConfigurationNames()) {
      System.out.println(
          "Loaded "
              + cm.getCache(destination).size()
              + " records from cache for destination:"
              + destination);
    }

    Instant start = Instant.now();
    for (int i = 0; i < rownumber; i++) {
      cache1.put(randomString(randomInt(31, 32)), randomString(randomInt(5300, 14300)));
      cache2.put(randomString(randomInt(31, 32)), randomString(randomInt(5300, 14300)));
      cache3.put(randomString(randomInt(31, 32)), randomString(randomInt(5300, 14300)));
    }

    System.out.println(
        "Stat:getCurrentNumberOfEntriesInMemory "
            + cm.getStats().getCurrentNumberOfEntriesInMemory());
    System.out.println("Cache Size - test: " + cache1.size());
    System.out.println("Cache Size - test2: " + cache2.size());
    System.out.println("Cache Size - test3: " + cache3.size());

    System.out.println(
        "Execution time of PUT seconds: " + Duration.between(start, Instant.now()).getSeconds());

    String tmp_hash = null;
    // 1111111
    start = Instant.now();
    cache1.startBatch();
    for (Object k : cache1.keySet()) {
      Object val = cache1.remove(k);
      tmp_hash = DigestUtils.md5Hex((String) val);
    }
    System.out.println("ignore uuid " + tmp_hash);
    cache1.endBatch(true);
    System.out.println(
        "Removal Execution time remove1 in seconds: "
            + Duration.between(start, Instant.now()).getSeconds());
    // 2222222
    start = Instant.now();
    Map<Object, Object> streamData2 =
        cache2.entrySet().parallelStream()
            .timeout(5, TimeUnit.MINUTES)
            .limit(rownumber)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    cache2.startBatch();
    for (Object k : cache2.keySet()) {
      Object val = cache2.remove(k);
      tmp_hash = DigestUtils.md5Hex((String) val);
    }
    System.out.println("ignore uuid " + tmp_hash);
    cache2.endBatch(true);
    System.out.println(
        "Removal Execution time remove2 in seconds: "
            + Duration.between(start, Instant.now()).getSeconds());

    // 3333333
    start = Instant.now();
    cache3.startBatch();
    cache3.entrySet().parallelStream()
        .timeout(5, TimeUnit.MINUTES)
        .limit(rownumber)
        .forEach(
            e -> {
              Object val = cache3.remove(e.getKey());
              final String tmp_hash2 = DigestUtils.md5Hex((String) val);
            });
    cache3.endBatch(true); // fastest

    System.out.println("ignore uuid " + "tmp_hash2");
    System.out.println(
        "Removal Execution time remove3 in seconds: "
            + Duration.between(start, Instant.now()).getSeconds());

    System.out.println("Cache Size - test: " + cache1.size());
    System.out.println("Cache Size - test2: " + cache2.size());
    System.out.println("Cache Size - test3: " + cache3.size());
  }
}
