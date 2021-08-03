package io.debezium.server.batch.uploadlock;

public interface InterfaceUploadLock {

  AutoCloseable lock(String destination) throws InterruptedException;
}
