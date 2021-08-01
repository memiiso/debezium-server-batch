package io.debezium.server.batch.uploadlock;

import io.debezium.server.batch.batchsizewait.MaxBatchSizeWait;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Alternative;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Dependent
@Alternative
public class LocalFileUploadLock implements InterfaceUploadLock {
  protected static final Logger LOGGER = LoggerFactory.getLogger(MaxBatchSizeWait.class);
  @ConfigProperty(name = "debezium.sink.batch.upload-lock.dir", defaultValue = "/tmp")
  String lockFileDir;
  @ConfigProperty(name = "debezium.sink.batch.upload-lock.max-wait-ms", defaultValue = "1800000")
  int maxWaitMs;
  @ConfigProperty(name = "debezium.sink.batch.upload-lock.wait-interval-ms", defaultValue = "10000")
  int waitIntervalMs;

  @Override
  public AutoCloseable lock(String destination) throws InterruptedException {
    final Path lockFile = Paths.get(lockFileDir, "upload-" + destination + ".lock");
    FileChannel channel;
    try {
      lockFile.toFile().createNewFile();
      channel = FileChannel.open(lockFile, StandardOpenOption.APPEND);
    } catch (IOException e) {
      throw new UploadLockException("Failed to create lock file!");
    }
    FileLock lock;
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
      } catch (IOException e) {
        throw new UploadLockException("Failed to take lock on file!");
      }
      if (totalWaitMs > maxWaitMs) {
        throw new UploadLockException("Timeout waiting to take lock on file " + lockFile);
      }
      LOGGER.debug("Waiting to take lock on file {}, file locked by another process", lockFile);
      totalWaitMs += waitIntervalMs;
      Thread.sleep(waitIntervalMs);
    }
    LOGGER.debug("Locked file {}", lockFile);
    return lock;
  }

}
