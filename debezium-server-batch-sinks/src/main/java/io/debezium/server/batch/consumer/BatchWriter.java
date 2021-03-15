/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.consumer;

import io.debezium.server.batch.BatchJsonlinesFile;

import java.io.IOException;

public interface BatchWriter {

  void uploadDestination(String destination, BatchJsonlinesFile jsonLinesFile);

  void close() throws IOException;

}
