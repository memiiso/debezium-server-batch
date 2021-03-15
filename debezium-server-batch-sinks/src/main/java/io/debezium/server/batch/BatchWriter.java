/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.engine.ChangeEvent;

import java.io.IOException;
import java.util.ArrayList;

public interface BatchWriter {

  void uploadDestination(String destination);

  void close() throws IOException;

  void appendAll(String destination, ArrayList<ChangeEvent<Object, Object>> records) throws InterruptedException;

}
