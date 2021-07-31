package io.debezium.server.batch.uploadlock;

import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Default;

@Dependent
@Default
public class NoUploadLock implements InterfaceUploadLock {
  @Override
  public AutoCloseable lock(String destination) {
    return null;
  }

}
