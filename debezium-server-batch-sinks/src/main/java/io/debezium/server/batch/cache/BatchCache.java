/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.cache;

import io.debezium.engine.ChangeEvent;
import io.debezium.server.batch.BatchJsonlinesFile;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface BatchCache {

  void close() throws IOException;

  void appendAll(String destination, List<ChangeEvent<Object, Object>> records) throws InterruptedException;

  BatchJsonlinesFile getJsonLines(String destination);

  Integer getEstimatedCacheSize(String destination);

  Set<String> getCaches();

  void initialize();

}
