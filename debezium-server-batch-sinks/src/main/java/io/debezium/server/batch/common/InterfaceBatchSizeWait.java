/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *  
 */

package io.debezium.server.batch.common;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */

public interface InterfaceBatchSizeWait {

  default void initizalize() {
  }

  void waitMs(long numRecordsProcessed, Integer processingTimeMs) throws InterruptedException;

}
