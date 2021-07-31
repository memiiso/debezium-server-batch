/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.uploadlock;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import java.util.concurrent.TimeoutException;
import javax.inject.Inject;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(LocalFileUploadLockTestProfile.class)
class LocalFileUploadLockTest {
  @Inject
  InterfaceUploadLock fileLock;
  @Inject
  InterfaceUploadLock fileLock2;

  @Test
  void shouldWaitForLock() throws Exception {
    fileLock.initizalize();
    fileLock2.initizalize();
    try (AutoCloseable l = fileLock.lock()) {
      Exception exception = assertThrows(TimeoutException.class, () -> {
        fileLock2.lock();
      });
      assertTrue(exception.getMessage().contains("Timeout waiting to take lock on file"));
    }
  }

}