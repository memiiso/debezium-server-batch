package io.debezium.server.batch.uploadlock;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import javax.enterprise.context.Dependent;

@Dependent
public interface InterfaceLock extends AutoCloseable {
  default void initizalize() throws IOException {
  }

  void lock() throws IOException, TimeoutException, InterruptedException;
}
