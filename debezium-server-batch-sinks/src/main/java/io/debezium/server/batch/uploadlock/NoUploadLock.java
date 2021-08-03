package io.debezium.server.batch.uploadlock;

import javax.enterprise.context.Dependent;
import javax.inject.Named;

@Dependent
@Named("NoUploadLock")
public class NoUploadLock implements InterfaceUploadLock {
  @Override
  public AutoCloseable lock(String destination) {
    return null;
  }

}
