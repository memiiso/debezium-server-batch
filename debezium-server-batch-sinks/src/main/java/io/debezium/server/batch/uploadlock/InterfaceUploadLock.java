package io.debezium.server.batch.uploadlock;

import javax.enterprise.context.Dependent;

@Dependent
public interface InterfaceUploadLock {

  AutoCloseable lock(String destination) throws InterruptedException;
}
