/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.manualtests;

import java.util.UUID;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class testInfinispan {
  protected static final Logger LOGGER = LoggerFactory.getLogger(testInfinispan.class);

  public static void main(String[] args) {
    Logger LOGGER = LoggerFactory.getLogger(testInfinispan.class);

    DefaultCacheManager cm = new DefaultCacheManager();
    //ConfigurationBuilder builder = new ConfigurationBuilder().simpleCache(true);
    ConfigurationBuilder builder = new ConfigurationBuilder();
    builder.simpleCache()
    //.persistence()
    //.passivation(false)
    //.addSingleFileStore()
    //preload : If true, when the cache starts, data stored in the cache store will be pre-loaded into memory
    //Can be used to provide a 'warm-cache' on startup, however there is a performance penalty as startup time is
    // affected by this process.
    //.preload(false)
    //.shared(false)
    //.fetchPersistentState(true)
    //.ignoreModifications(false)
    //.purgeOnStartup(true) // If true, purges this cache store when it starts up.
    //.location("./cache")
    //.async()
    //.enabled(true);
//      builder.simpleCache(true)
    ;

    cm.start();
    cm.defineConfiguration("mySimpleCache", builder.build());
    cm.defineConfiguration("mySimpleCache2", builder.build());
    cm.defineConfiguration("mySimpleCache3", builder.build());

    Cache cache = cm.getCache("mySimpleCache");
    Cache cache2 = cm.getCache("mySimpleCache2");
    Cache cache3 = cm.getCache("mySimpleCache3");

    cache.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    cache2.put("key2", "val2");
    System.out.println(cm.getCacheConfigurationNames());
    cache.values().stream().forEach(System.out::println);
    System.out.println("----------------------");
    cache.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    cache.put("mykey", "val1");
    cache.put("mykey", "val2");
    cache.values().stream().forEach(System.out::println);
    System.out.println("----------------------");
    cache.remove("mykey");
    cache.values().stream().forEach(System.out::println);
    System.out.println("----------------------");
  }
}
