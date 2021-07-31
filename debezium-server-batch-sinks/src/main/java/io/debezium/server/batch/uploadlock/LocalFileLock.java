package io.debezium.server.batch.uploadlock;

import io.debezium.server.batch.dynamicwait.MaxBatchSizeWait;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
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
public class LocalFileLock implements InterfaceLock {
  protected static final Logger LOGGER = LoggerFactory.getLogger(MaxBatchSizeWait.class);
  Path pathLockFile;
  @ConfigProperty(name = "debezium.sink.batch.upload-lock.file",
      defaultValue = "/tmp/debezium-batch-upload-lock.lock")
  String lockFile;
  @ConfigProperty(name = "debezium.sink.batch.upload-lock.max-wait-ms", defaultValue = "1800000")
  int maxWaitMs;
  @ConfigProperty(name = "debezium.sink.batch.upload-lock.wait-interval-ms", defaultValue = "10000")
  int waitIntervalMs;
  FileChannel channel;

  @Override
  public void initizalize() throws IOException {
    pathLockFile = Paths.get(lockFile);
    pathLockFile.toFile().createNewFile();
  }

  @Override
  public AutoCloseable lock() throws TimeoutException, InterruptedException, IOException {
    channel = FileChannel.open(pathLockFile, StandardOpenOption.APPEND);
    FileLock lock = null;
    int totalWaitMs = 0;
    LOGGER.debug("Locking file {}", lockFile);
    while (true) {
      try {
        lock = channel.tryLock();
        if (lock != null) {
          break;
        }
      } catch (OverlappingFileLockException e) {
        //
      }
      if (totalWaitMs > maxWaitMs) {
        throw new TimeoutException("Timeout waiting to take lock on file " + lockFile);
      }
      LOGGER.debug("Waiting to take lock on file {}, file locked by another process", lockFile);
      totalWaitMs += waitIntervalMs;
      Thread.sleep(waitIntervalMs);
    }
    LOGGER.debug("Locked file {}", lockFile);
    return lock;
  }

  @Override
  public void close() throws Exception {
    channel.close();
  }
}
