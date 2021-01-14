/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.batch.batchwriter;

import java.io.File;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.ConfigProvider;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;

import io.debezium.server.batch.keymapper.ObjectKeyMapper;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public abstract class AbstractBatchRecordWriter implements BatchRecordWriter, AutoCloseable {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractBatchRecordWriter.class);
    protected final File TEMPDIR = Files.createTempDir();
    protected final ObjectKeyMapper objectKeyMapper;
    protected final DB cdcDb;
    protected final ConcurrentMap<String, String> map_data;
    protected final ConcurrentMap<String, Integer> map_batchid;
    protected final ObjectMapper jsonMapper = new ObjectMapper();
    // @TODO check param naming!
    final Integer batchLimit = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.row.limit", Integer.class).orElse(500);
    final ScheduledExecutorService timerExecutor = Executors.newSingleThreadScheduledExecutor();
    protected LocalDateTime batchTime = LocalDateTime.now();

    public AbstractBatchRecordWriter(ObjectKeyMapper mapper) throws URISyntaxException {
        this.objectKeyMapper = mapper;
        // init db
        this.cdcDb = DBMaker
                .fileDB(TEMPDIR.toPath().resolve("debeziumevents.db").toFile())
                .fileMmapEnable()
                .transactionEnable()
                .closeOnJvmShutdown()
                .fileDeleteAfterClose()
                .make();
        map_data = cdcDb
                .hashMap("map_data", Serializer.STRING, Serializer.STRING)
                .createOrOpen();
        map_batchid = cdcDb
                .hashMap("map_batchid", Serializer.STRING, Serializer.INTEGER)
                .createOrOpen();

        LOGGER.info("Set Batch Row limit to {} Rows", batchLimit);
        LOGGER.info("Local Cache (MapDb) Location:{}", TEMPDIR.toPath().resolve("debeziumevents.db").toAbsolutePath().toString());
        // DISABLED! this can be achieved using poll.interval.ms and max.batch.size
        // poll.interval.ms = Positive integer value that specifies the number of milliseconds the connector should wait during each iteration for new change events to appear. Defaults to 1000 milliseconds,
        // or 1 second.
        // setupTimer();
    }

    // DISABLED! this can be achieved using poll.interval.ms and max.batch.size
    protected void setupTimer() {
        final Integer timerBatchLimit = ConfigProvider.getConfig().getOptionalValue("debezium.sink.batch.time.limit", Integer.class).orElse(3600);
        LOGGER.info("Set Batch Time limit to {} Second", timerBatchLimit);
        Runnable timerTask = () -> {
            LOGGER.debug("Timer is up uploading batch data!");
            try {
                this.uploadBatch();
            }
            catch (Exception e) {
                LOGGER.error("Timer based batch upload failed data will be uploaded with next batch!");
            }
        };
        timerExecutor.scheduleWithFixedDelay(timerTask, timerBatchLimit, timerBatchLimit, TimeUnit.SECONDS);
    }

    @Override
    public void append(String destination, JsonNode valueJson) throws JsonProcessingException {

        if (!map_data.containsKey(destination)) {
            map_data.put(destination, jsonMapper.writeValueAsString(valueJson).replace("\n", ""));
            map_batchid.putIfAbsent(destination, 0);
            cdcDb.commit();
            return;
        }

        map_data.put(destination, map_data.get(destination) + IOUtils.LINE_SEPARATOR + jsonMapper.writeValueAsString(valueJson).replace("\n", ""));

        if (StringUtils.countMatches(map_data.get(destination), IOUtils.LINE_SEPARATOR) >= batchLimit) {
            LOGGER.debug("Batch Row Limit reached Uploading Data, destination:{} batchId:{}", destination, map_batchid.get(destination));
            this.uploadBatchFile(destination);
        }
        cdcDb.commit();
    }

    protected void uploadBatchFile(String destination) {
        throw new NotImplementedException("Not Implemented!");
    }

    @Override
    public void uploadBatch() {
        int numBatchFiles = 0;
        for (String k : map_data.keySet()) {
            uploadBatchFile(k);
            numBatchFiles++;
        }
        this.setBatchTime();
        LOGGER.info("Uploaded {} Batch Files, started new batch Batch Time:{}", numBatchFiles, this.batchTime.toEpochSecond(ZoneOffset.UTC));
        // if (!map_data.isEmpty()) {
        // LOGGER.error("Non Processed Batch Data Found batchTime:{} destination: {}!!", batchTime.toString(), map_data.keySet().toString());
        // }
    }

    @Override
    public void close() {
        stopTimer();
        if (!cdcDb.isClosed()) {
            this.uploadBatch();
            closeDb();
        }
        TEMPDIR.delete();
    }

    protected void setBatchTime() {
        batchTime = LocalDateTime.now();
    }

    protected void closeDb() {
        if (!cdcDb.isClosed()) {
            // upload data second time
            if (!map_data.isEmpty()) {
                this.uploadBatch();
            }
            if (!map_data.isEmpty()) {
                LOGGER.error("Non Processed Batch Data Found!");
            }
            else {
                LOGGER.info("All The Data Successfully Processed.");
            }

            LOGGER.info("Closing Batch Consumer({})", this.getClass().getName());
            cdcDb.close();
        }
    }

    protected void stopTimer() {
        timerExecutor.shutdown();
        try {
            if (!timerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                timerExecutor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            LOGGER.error("Timer Shutting Down Failed {}", e.getMessage());
            timerExecutor.shutdownNow();
        }
    }
}
