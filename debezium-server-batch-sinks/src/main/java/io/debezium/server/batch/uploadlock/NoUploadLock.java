package io.debezium.server.batch.uploadlock;

import java.io.IOException;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Default;

@Dependent
@Default
public class NoUploadLock implements InterfaceUploadLock {
  @Override
  public AutoCloseable lock() throws IOException {
    return null;
  }

  @Override
  public void close() throws Exception {
  }
}
