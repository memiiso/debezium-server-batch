/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.uploadlock;


public class UploadLockException extends InterruptedException {

  public UploadLockException(String m) {
    super(m);
  }
}