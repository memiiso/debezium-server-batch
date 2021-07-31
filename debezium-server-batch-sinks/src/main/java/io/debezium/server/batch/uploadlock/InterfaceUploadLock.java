package io.debezium.server.batch.uploadlock;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import javax.enterprise.context.Dependent;

@Dependent
public interface InterfaceUploadLock extends AutoCloseable {
  default void initizalize() throws IOException {
  }

  AutoCloseable lock() throws IOException, TimeoutException, InterruptedException;
}
