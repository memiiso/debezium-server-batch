/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.server.BaseChangeConsumer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("batch")
@Dependent
public class BatchChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOG = LoggerFactory.getLogger(BatchChangeConsumer.class);

    @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
    String valueFormat;

    @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
    String keyFormat;

    @Inject
    BatchWriter batchWriter;

    @Inject
    @ConfigProperty(name = "debezium.sink.batch.writer")
    String customBatchWriter;

    @Inject
    @ConfigProperty(name = "debezium.sink.batch.cache.use-batch-append", defaultValue = "true")
    Boolean useBatchAppend;

    @PreDestroy
    void close() {
        try {
            LOG.info("Closing batch writer!");
            batchWriter.close();
        } catch (Exception e) {
            LOG.warn("Exception while closing writer:{} ", e.getMessage());
            e.printStackTrace();
        }
    }

    @PostConstruct
    void connect() throws URISyntaxException, InterruptedException {

        LOG.info("Using '{}' batch writer", batchWriter.getClass().getName());

        if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
            throw new InterruptedException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) formats are {json,}!");
        }
        if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
            throw new InterruptedException("debezium.format.key={" + valueFormat + "} not supported! Supported (debezium.format.key=*) formats are {json,}!");
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        if (useBatchAppend) {


            Map<String, ArrayList<ChangeEvent<Object, Object>>> result = records.stream()
                    .collect(Collectors.groupingBy(
                            ChangeEvent::destination,
                            Collectors.mapping(p -> p,
                                    Collectors.toCollection(ArrayList::new))));

            for (Map.Entry<String, ArrayList<ChangeEvent<Object, Object>>> destinationEvents : result.entrySet()) {
                batchWriter.appendAll(destinationEvents.getKey(), destinationEvents.getValue());
            }
            // workaround! somehow offset is not saved to file unless we call committer.markProcessed
            // even its should be saved to file periodically
            if (!records.isEmpty()) {
                committer.markProcessed(records.get(0));
            }
        } else {
            for (ChangeEvent<Object, Object> record : records) {
                batchWriter.append(record.destination(), record);
                committer.markProcessed(record);
            }
        }
        committer.markBatchFinished();
    }

}