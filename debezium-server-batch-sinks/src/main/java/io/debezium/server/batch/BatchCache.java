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

public interface BatchCache {

  void close() throws IOException;

  void append(ChangeEvent<Object, Object> record);
}
