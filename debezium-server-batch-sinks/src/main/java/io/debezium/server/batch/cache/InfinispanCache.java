/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.cache;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Default;

import org.eclipse.microprofile.config.ConfigProvider;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.GlobalMetricsConfiguration;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Dependent
@Default
public class InfinispanCache extends AbstractCache {

  private static final Logger LOG = LoggerFactory.getLogger(InfinispanCache.class);
  protected static final String cacheStore =
      ConfigProvider.getConfig()
          .getOptionalValue("debezium.sink.batch.cache-store", String.class)
          .orElse("local");
  protected static final String cacheLocation =
      ConfigProvider.getConfig()
          .getOptionalValue("debezium.sink.batch.cache.location", String.class)
          .orElse("cache");
  protected static final long memoryMaxCount =
      ConfigProvider.getConfig()
          .getOptionalValue("debezium.sink.batch.cache.memory-maxcount", long.class)
          .orElse(MemoryConfiguration.MAX_COUNT.getDefaultValue());
  protected static final Integer maxBatchSize =
      ConfigProvider.getConfig()
          .getOptionalValue("debezium.sink.batch.cache.max-batch-size", Integer.class)
          .orElse(1024);
  protected static final Boolean purgeOnStartup =
      ConfigProvider.getConfig()
          .getOptionalValue("debezium.sink.batch.cache.purge-on-startup", Boolean.class)
          .orElse(AbstractStoreConfiguration.PURGE_ON_STARTUP.getDefaultValue());
  protected static final Boolean invocationBatching =
      ConfigProvider.getConfig()
          .getOptionalValue("debezium.sink.batch.cache.invocation-batching", Boolean.class)
          .orElse(false);

  protected static final boolean statistics =
      ConfigProvider.getConfig()
          .getOptionalValue("debezium.sink.batch.cache.statistics", boolean.class)
          .orElse(false);
  protected static final boolean metricsGauges =
      ConfigProvider.getConfig()
          .getOptionalValue("debezium.sink.batch.cache.metrics-gauges", boolean.class)
          .orElse(GlobalMetricsConfiguration.GAUGES.getDefaultValue());
  protected static final boolean metricsHistograms =
      ConfigProvider.getConfig()
          .getOptionalValue("debezium.sink.batch.cache.metrics-histograms", boolean.class)
          .orElse(GlobalMetricsConfiguration.HISTOGRAMS.getDefaultValue());
  protected static final boolean unreliableReturnValues =
      ConfigProvider.getConfig()
          .getOptionalValue("debezium.sink.batch.cache.unreliable-return-values", boolean.class)
          .orElse(false);

  protected static final ConcurrentHashMap<String, Integer> cacheRowCounter =
      new ConcurrentHashMap<>();
  protected final DefaultCacheManager cm =
      new DefaultCacheManager(
          new GlobalConfigurationBuilder()
              .cacheContainer()
              .statistics(statistics)
              .metrics()
              .gauges(metricsGauges)
              .histograms(metricsHistograms)
              // disable shutdown hook because we will will do it gracefully
              .shutdown()
              .hookBehavior(ShutdownHookBehavior.DONT_REGISTER)
              .build());
  final ConfigurationBuilder builder = new ConfigurationBuilder();

  public InfinispanCache() {
    super();

    boolean preloadCache = true;
    if (purgeOnStartup) {
      preloadCache = false;
    }

    if (!cacheStore.equalsIgnoreCase("simple")) {
      builder
          .memory()
          .maxCount(memoryMaxCount)
          .invocationBatching()
          .enable(invocationBatching)
          .unsafe()
          .unreliableReturnValues(unreliableReturnValues);
      LOG.info("Infinispan cache unreliableReturnValues set to {}", unreliableReturnValues);
      LOG.info("Infinispan cache invocationBatching set to {}", invocationBatching);
      LOG.info("Infinispan cache memory MaxCount set to {}", memoryMaxCount);
      LOG.info("Infinispan cache purgeOnStartup set to {}", purgeOnStartup);
      LOG.info("Infinispan cache maxBatchSize set to {}", maxBatchSize);
    }

    if (cacheStore.equalsIgnoreCase("simple")) {
      LOG.info("Using Infinispan simple cache");
      builder.simpleCache(true);
    } else if (cacheStore.equalsIgnoreCase("local")) {
      LOG.info("Using Infinispan local cache, location: {}", cacheLocation);
      builder
          // PersistenceConfigurationBuilder
          .persistence()
          // Adds a single file cache store.. Local (non-shared) file store
          .addSingleFileStore()
          // If true, when the cache starts, data stored in the cache store will be pre-loaded into
          // memory.
          .preload(preloadCache)
          .maxBatchSize(maxBatchSize)
          // If true, purges this cache store when it starts up.
          .purgeOnStartup(purgeOnStartup)
          // Sets a location on disk where the store can write.
          .location(cacheLocation + "/local");
    } else if (cacheStore.equalsIgnoreCase("rocksdb")) {
      LOG.info("Using Infinispan RocksDB cache, location: {}", cacheLocation);
      Properties props = new Properties();
      props.put("database.max_background_compactions", "4");
      props.put("data.write_buffer_size", "512MB");
      builder
          // PersistenceConfigurationBuilder
          .persistence()
          .addStore(RocksDBStoreConfigurationBuilder.class)
          .location(cacheLocation + "/rocksdb/data")
          .expiredLocation(cacheLocation + "/rocksdb/expired")
          .properties(props)
          .maxBatchSize(maxBatchSize)
          .preload(preloadCache)
          // If true, purges this cache store when it starts up.
          .purgeOnStartup(purgeOnStartup);
    } else {
      throw new DebeziumException("Cache store'" + cacheStore + "' not supported!");
    }
    LOG.info("Infinispan statistics set to {}", statistics);
    LOG.info("Infinispan metricsGauges set to {}", metricsGauges);
    LOG.info("Infinispan metricsHistograms set to {}", metricsHistograms);

    LOG.debug("Starting cache manager");
    cm.start();
  }

  private Cache<Object, Object> getDestinationCache(String destination) {
    // create cache for the destination if not exists
    if (!cm.cacheExists(destination)) {
      LOG.debug("Creating cache for destination:{}", destination);
      cm.defineConfiguration(destination, builder.build());
      cacheRowCounter.merge(destination, cm.getCache(destination).size(), Integer::sum);

      // if cache has old data print info log
      if (cacheRowCounter.getOrDefault(destination, 0) > 0) {
        LOG.info(
            "Loaded previous {} records from cache for destination:{}",
            cacheRowCounter.get(destination),
            destination);
      }
    }
    return cm.getCache(destination);
  }

  @Override
  public void append(String destination, ChangeEvent<Object, Object> record) {
    /*synchronized (cacheUpdateLock.computeIfAbsent(destination, k -> new Object()))*/
    {
      Cache<Object, Object> cache = this.getDestinationCache(destination);
      // append record to cache
      final String key = UUID.randomUUID().toString();
      cache.put(key, record.value());
      if (LOG.isTraceEnabled()) {
        LOG.trace("Cache.append key:'{}' val:{}", key, record.value());
      }
      cacheRowCounter.merge(destination, 1, Integer::sum);
    }
  }

  @Override
  public void appendAll(String destination, ArrayList<ChangeEvent<Object, Object>> records) {
    /*synchronized (cacheUpdateLock.computeIfAbsent(destination, k -> new Object()))*/
    {
      // collect only event values
      Map<String, Object> destinationEventVals =
          records.stream()
              .collect(
                  // in case of conflict keep existing
                  Collectors.toMap(
                      x -> UUID.randomUUID().toString(),
                      ChangeEvent::value,
                      (existing, replacement) -> existing));
      Cache<Object, Object> cache = this.getDestinationCache(destination);

      if (LOG.isTraceEnabled()) {
        for (Map.Entry<String, Object> e : destinationEventVals.entrySet()) {
          LOG.trace("Cache.appendAll key:'{}' val:{}", e.getKey(), e.getValue().toString());
        }
      }

      cache.putAll(destinationEventVals);
      cacheRowCounter.merge(destination, destinationEventVals.size(), Integer::sum);
      destinationEventVals.clear();
      records.clear();
    }
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing cache");
    try {
      cm.stop();
    } catch (Exception e) {
      LOG.warn("Exception during cache shutdown ", e);
    }
    LOG.debug("Closed cache");
  }

  public BatchJsonlinesFile getJsonLines(String destination) {

    synchronized (cacheUpdateLock.computeIfAbsent(destination, k -> new Object())) {
      JsonNode schema = null;
      boolean isFirst = true;
      final File tempFile;
      int processedNumber = 0;
      Cache<Object, Object> cache = cm.getCache(destination);
      try {
        tempFile = File.createTempFile(UUID.randomUUID() + "-", ".json");
        FileOutputStream fos = new FileOutputStream(tempFile, true);

        if (cache.getCacheConfiguration().invocationBatching().enabled()) {
          LOG.info("Cache Invocation batching enabled starting invocation batch");
          cache.startBatch();
        }

        for (Map.Entry<Object, Object> e : cache.entrySet()) {
          Object val = cache.get(e.getKey());

          // this could happen if multiple threads reading and removing data
          if (val == null) {
            LOG.debug(
                "Cache.getJsonLines Null Value returned for key:'{}' destination:'{}'! "
                    + "skipping the entry!",
                e.getKey(),
                destination);
            continue;
          }
          LOG.trace("Cache.getJsonLines key:'{}' val:{}", e.getKey(), getString(val));

          if (isFirst) {
            schema = this.getJsonSchema(val);
            isFirst = false;
          }

          try {
            final JsonNode valNode = valDeserializer.deserialize(destination, getBytes(val));
            final String valData = mapper.writeValueAsString(valNode) + System.lineSeparator();

            if (LOG.isTraceEnabled()) {
              LOG.trace(
                  "Cache.getJsonLines key:'{}' val Json Node:{}", e.getKey(), valNode.toString());
              LOG.trace("Cache.getJsonLines key:'{}' val String:{}", e.getKey(), valData);
            }

            fos.write(valData.getBytes(StandardCharsets.UTF_8));
          } catch (IOException ioe) {
            LOG.error("Failed writing record to file", ioe);
            fos.close();
            throw new UncheckedIOException(ioe);
          }
          cache.removeAsync(e.getKey());
          processedNumber += 1;
          if (processedNumber >= batchRowLimit) {
            break;
          }
        }

        fos.close();

        if (cache.getCacheConfiguration().invocationBatching().enabled()) {
          cache.endBatch(true);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      // if nothing processed return null
      if (isFirst) {
        tempFile.delete();
        return null;
      }

      cacheRowCounter.merge(destination, -processedNumber, Integer::sum);
      return new BatchJsonlinesFile(tempFile, schema);
    }
  }

  @Override
  public Integer getEstimatedCacheSize(String destination) {
    return cacheRowCounter.getOrDefault(destination, 0);
  }

  @Override
  public Set<String> getCaches() {
    return cm.getCacheConfigurationNames();
  }
}