/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.cache;

import io.debezium.engine.ChangeEvent;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Alternative;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */

@Dependent
@Alternative
public class MemoryCache extends AbstractCache {

  protected static final Logger LOGGER = LoggerFactory.getLogger(MemoryCache.class);
  Map<String, ConcurrentHashMap<String, Object>> cacheManager = new ConcurrentHashMap<>();

  public MemoryCache() {
    super();
    LOGGER.info("Using MemoryCache (ConcurrentHashMap) as in memory cache");
  }

  private ConcurrentHashMap<String, Object> getDestinationCache(String destination) {
    return cacheManager.computeIfAbsent(destination, k -> new ConcurrentHashMap<String, Object>());
  }

  @Override
  public void append(String destination, ChangeEvent<Object, Object> record) {

    // serialize receiving and luploading records. to prevent out of memory issues
    synchronized (cacheUpdateLock.computeIfAbsent(destination, k -> new Object())) {
      ConcurrentHashMap<String, Object> cache = this.getDestinationCache(destination);
      final String key = UUID.randomUUID().toString();
      cache.putIfAbsent(key, record.value());
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Cache.append key:'{}' val:{}", key, record.value());
      }
    }
  }

  @Override
  public void appendAll(String destination, ArrayList<ChangeEvent<Object, Object>> records) {

    // serialize receiving and luploading records. to prevent out of memory issues
    synchronized (cacheUpdateLock.computeIfAbsent(destination, k -> new Object())) {
      //collect only event values
      Map<String, Object> destinationEventVals = records.stream()
          .collect(
              // in case of conflict keep existing
              Collectors.toMap(x -> UUID.randomUUID().toString(), ChangeEvent::value, (existing, replacement) -> existing)
          );
      ConcurrentHashMap<String, Object> cache = this.getDestinationCache(destination);

      if (LOGGER.isTraceEnabled()) {
        for (Map.Entry<String, Object> e : destinationEventVals.entrySet()) {
          LOGGER.trace("Cache.appendAll key:'{}' val:{}", e.getKey(), e.getValue().toString());
        }
      }

      cache.putAll(destinationEventVals);
      destinationEventVals.clear();
      records.clear();
    }
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("Closing cache");
    for (String c : this.getCaches()) {
      LOGGER.info("For destination:{} {} unsent record discarded", c, this.getDestinationCache(c).size());
    }
  }

  public BatchJsonlinesFile getJsonLines(String destination) {


    synchronized (cacheUpdateLock.computeIfAbsent(destination, k -> new Object())) {
      JsonNode schema = null;
      boolean isFirst = true;
      final File tempFile;
      ConcurrentHashMap<String, Object> cache = this.getDestinationCache(destination);
      try {
        tempFile = File.createTempFile(UUID.randomUUID() + "-", ".json");
        FileOutputStream fos = new FileOutputStream(tempFile, true);

        for (Map.Entry<String, Object> e : cache.entrySet()) {
          Object val = cache.get(e.getKey());

          // this could happen if multiple threads reading and removing data
          if (val == null) {
            LOGGER.debug("Cache.getJsonLines Null Value returned for key:'{}' destination:'{}'! " +
                    "skipping the entry!",
                e.getKey(), destination);
            continue;
          }
          LOGGER.trace("Cache.getJsonLines key:'{}' val:{}", e.getKey(), getString(val));

          if (isFirst) {
            schema = this.getJsonSchema(val);
            isFirst = false;
          }

          try {
            final JsonNode valNode = valDeserializer.deserialize(destination, getBytes(val));
            final String valData = mapper.writeValueAsString(valNode) + System.lineSeparator();

            if (LOGGER.isTraceEnabled()) {
              LOGGER.trace("Cache.getJsonLines key:'{}' val Json Node:{}", e.getKey(), valNode.toString());
              LOGGER.trace("Cache.getJsonLines key:'{}' val String:{}", e.getKey(), valData);
            }

            fos.write(valData.getBytes(StandardCharsets.UTF_8));
          } catch (IOException ioe) {
            LOGGER.error("Failed writing record to file", ioe);
            fos.close();
            throw new UncheckedIOException(ioe);
          }
          cache.remove(e.getKey());
        }

        fos.close();

      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      // if nothing processed return null
      if (isFirst) {
        tempFile.delete();
        return null;
      }

      return new BatchJsonlinesFile(tempFile, schema);
    }

  }

  @Override
  public Integer getEstimatedCacheSize(String destination) {
    return this.getDestinationCache(destination).size();
  }

  @Override
  public Set<String> getCaches() {
    return cacheManager.keySet();
  }

}



