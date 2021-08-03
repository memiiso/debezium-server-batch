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
class NoUploadLockTest {
  @Inject
  NoUploadLock noLock;
  @Inject
  NoUploadLock noLock2;

  @Test
  void shouldNotWaitForLock() throws Exception {
    try (AutoCloseable l = noLock.lock("test.table")) {
      try (AutoCloseable l2 = noLock2.lock("test.table")) {
      }
    }
  }

}