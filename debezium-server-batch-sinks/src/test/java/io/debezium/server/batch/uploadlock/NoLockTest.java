/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.uploadlock;

import io.quarkus.test.junit.QuarkusTest;

import javax.inject.Inject;

import org.junit.jupiter.api.Test;

@QuarkusTest
class NoLockTest {
  @Inject
  InterfaceLock noLock;
  @Inject
  InterfaceLock noLock2;

  @Test
  void shouldNotWaitForLock() throws Exception {
    noLock.initizalize();
    noLock2.initizalize();
    try (AutoCloseable l = noLock.lock()) {
      noLock2.lock();
    }
  }

}