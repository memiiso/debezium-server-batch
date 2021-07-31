package io.debezium.server.batch.uploadlock;

import io.debezium.server.batch.dynamicwait.MaxBatchSizeWait;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeoutException;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Alternative;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Dependent
@Alternative
public class FileLock implements InterfaceLock {
  protected static final Logger LOGGER = LoggerFactory.getLogger(MaxBatchSizeWait.class);
  Path pathLockFile;
  @ConfigProperty(name = "debezium.sink.batch.upload-lock.file", defaultValue = "/tmp/debezium-sparkbatchFileLock.txt")
  String lockFile;
  @ConfigProperty(name = "debezium.sink.batch.upload-lock.max-wait-ms", defaultValue = "1800000")
  int maxWaitMs;
  @ConfigProperty(name = "debezium.sink.batch.upload-lock.wait-interval-ms", defaultValue = "10000")
  int waitIntervalMs;
  FileChannel channel;

  public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
    new FileLock().lock();
  }

  @Override
  public void initizalize() throws IOException {
    Path pathLockFile = Paths.get(lockFile);
    pathLockFile.toFile().createNewFile();
  }

  @Override
  public void lock() throws IOException, TimeoutException, InterruptedException {
    channel = FileChannel.open(pathLockFile, StandardOpenOption.APPEND);
    java.nio.channels.FileLock lock = null;
    int totalWaitMs = 0;
    LOGGER.debug("Locking file {}", lockFile);
    while (lock == null) {
      lock = channel.tryLock();
      if (totalWaitMs > maxWaitMs) {
        throw new TimeoutException("Timeout waiting for the lock!");
      }
      totalWaitMs += waitIntervalMs;
      Thread.sleep(waitIntervalMs);
    }
    LOGGER.debug("Locked file {}", lockFile);
  }

  @Override
  public void close() throws Exception {
    channel.close();
  }
}
