package io.debezium.server.batch.uploadlock;

import java.io.IOException;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Default;

@Dependent
@Default
public class NoLock implements InterfaceLock {
  @Override
  public void lock() throws IOException {
  }

  @Override
  public void close() throws Exception {
  }
}
