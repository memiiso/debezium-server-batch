/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.consumer;

import io.debezium.engine.ChangeEvent;
import io.debezium.server.batch.BatchCache;
import io.debezium.server.batch.BatchWriter;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public abstract class AbstractConsumer implements BatchWriter {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractConsumer.class);

    @ConfigProperty(name = "debezium.sink.batch.cache", defaultValue = "infinispan")
    String cacheStore;

    @ConfigProperty(name = "debezium.sink.batch.objectkey-partition", defaultValue = "false")
    Boolean partitionData;

    @ConfigProperty(name = "debezium.sink.batch.objectkey-prefix")
    String objectKeyPrefix;

    @ConfigProperty(name = "debezium.sink.batch.time-limit", defaultValue = "600")
    Integer batchInterval;

    @ConfigProperty(name = "debezium.sink.batch.row-limit", defaultValue = "500")
    Integer batchUploadRowLimit;

    @Inject
    protected BatchCache cache;

    @Inject
    protected ConcurrentThreadPoolExecutor threadPool;

    final ScheduledExecutorService timerExecutor = Executors.newSingleThreadScheduledExecutor();

    public AbstractConsumer() {
        setupTimerUpload();
        LOG.info("Batch row limit set to {}", batchUploadRowLimit);
    }

    protected String getPartition() {
        final LocalDateTime batchTime = LocalDateTime.now();
        return "year=" + batchTime.getYear() + "/month=" + StringUtils.leftPad(batchTime.getMonthValue() + "", 2, '0') + "/day="
                + StringUtils.leftPad(batchTime.getDayOfMonth() + "", 2, '0');
    }

    public String map(String destination) {
        Objects.requireNonNull(destination, "destination Cannot be Null");
        if (partitionData) {
            String partitioned = getPartition();
            return objectKeyPrefix + destination + "/" + partitioned;
        } else {
            return objectKeyPrefix + destination;
        }
    }

    @Override
    public void append(String destination, ChangeEvent<Object, Object> record) throws InterruptedException {
        cache.append(destination, record);
        this.startUploadIfRowLimitReached(destination);
    }

    @Override
    public void appendAll(String destination, ArrayList<ChangeEvent<Object, Object>> records) throws InterruptedException {
        cache.appendAll(destination, records);
        this.startUploadIfRowLimitReached(destination);
    }

    private void startUploadIfRowLimitReached(String destination) {
        // get count per destination
        if (cache.getEstimatedCacheSize(destination) < batchUploadRowLimit) {
            return;
        }

        Thread uploadThread = new Thread(() -> {
            Thread.currentThread().setName("spark-row-limit-upload-" + Thread.currentThread().getId());
            // data might be already processed
            if (this.cache.getEstimatedCacheSize(destination) < batchUploadRowLimit) {
                return;
            }
            LOG.debug("Batch row limit reached, cache.size > batchLimit {}>={}, starting upload destination:{}",
                    cache.getEstimatedCacheSize(destination), batchUploadRowLimit, destination);

            this.uploadDestination(destination);
            LOG.debug("Finished Upload Thread:{}", Thread.currentThread().getName());
        });
        threadPool.submit(destination, uploadThread);
    }

    private void startTimerUpload(String destination) {
        // divide it to batches and upload
        for (int i = 0; i <= (this.cache.getEstimatedCacheSize(destination) / batchUploadRowLimit) + 1; i++) {

            Thread uploadThread = new Thread(() -> {
                Thread.currentThread().setName("spark-timer-upload-" + Thread.currentThread().getId());
                this.uploadDestination(destination);
                LOG.debug("Finished Upload Thread:{}", Thread.currentThread().getName());
            });
            threadPool.submit(destination, uploadThread);

        }
    }

    protected void setupTimerUpload() {
        LOG.info("Batch time limit set to {} second", batchInterval);
        // Runnable timerTask = () -> {
        Thread timerTask = new Thread(() -> {
            try {
                Thread.currentThread().setName("timer-upload-" + Thread.currentThread().getId());
                LOG.info("Timer upload, uploading all cache data(all destinations)!");
                // get cachedestination
                for (String k : cache.getCaches()) {
                    this.startTimerUpload(k);
                }

            } catch (Exception e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
                throw new RuntimeException("Timer based data upload failed!", e);
            }
        });
        timerExecutor.scheduleWithFixedDelay(timerTask, batchInterval, batchInterval, TimeUnit.SECONDS);
    }

    protected void stopTimerUpload() {
        try {
            LOG.info("Stopping timer task");
            timerExecutor.shutdown();

            if (!timerExecutor.awaitTermination(3, TimeUnit.MINUTES)) {
                LOG.warn("Timer did not terminate in the specified time(3m).");
                List<Runnable> droppedTasks = timerExecutor.shutdownNow();
                LOG.warn("Executor was abruptly shut down. " + droppedTasks.size() + " tasks will not be executed.");
            } else {
                LOG.debug("Stopped timer");
                LOG.info("Stopped timer");
            }
        } catch (Exception e) {
            LOG.error("Timer shutdown failed {}", e.getMessage());
        }
    }

    protected void stopUploadQueue() {
        threadPool.shutdown();
    }

}
