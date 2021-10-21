/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.batch.batchsizewait.InterfaceBatchSizeWait;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;
import javax.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public abstract class AbstractBatchChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  protected static final Duration LOG_INTERVAL = Duration.ofMinutes(15);
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
  protected final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  protected final ObjectMapper mapper = new ObjectMapper();
  protected Deserializer<JsonNode> valDeserializer;

  @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
  String valueFormat;

  @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
  String keyFormat;

  @ConfigProperty(name = "debezium.sink.batch.batch-size-wait", defaultValue = "NoBatchSizeWait")
  String batchSizeWaitName;

  @Inject
  @Any
  Instance<InterfaceBatchSizeWait> batchSizeWaitInstances;
  InterfaceBatchSizeWait batchSizeWait;
  protected Clock clock = Clock.system();
  protected long consumerStart = clock.currentTimeInMillis();
  protected long numConsumedEvents = 0;
  protected Threads.Timer logTimer = Threads.timer(clock, LOG_INTERVAL);

  public void initizalize() throws InterruptedException {

    valSerde.configure(Collections.emptyMap(), false);
    valDeserializer = valSerde.deserializer();

    if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) formats are {json,}!");
    }

    if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.key={" + valueFormat + "} not supported! Supported (debezium.format.key=*) formats are {json,}!");
    }

    Instance<InterfaceBatchSizeWait> instance = batchSizeWaitInstances.select(NamedLiteral.of(batchSizeWaitName));
    if (instance.isAmbiguous()) {
      throw new DebeziumException("Multiple batch size wait class named '" + batchSizeWaitName + "' were found");
    } else if (instance.isUnsatisfied()) {
      throw new DebeziumException("No batch size wait class named '" + batchSizeWaitName + "' is available");
    }
    batchSizeWait = instance.get();
    batchSizeWait.initizalize();
    LOGGER.info("Using {}", batchSizeWait.getClass().getName());
  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {
    LOGGER.trace("Received {} events", records.size());

    Instant start = Instant.now();
    Map<String, ArrayList<ChangeEvent<Object, Object>>> result = records.stream()
        .collect(Collectors.groupingBy(
            ChangeEvent::destination,
            Collectors.mapping(p -> p,
                Collectors.toCollection(ArrayList::new))));

    long numUploadedEvents = 0;
    for (Map.Entry<String, ArrayList<ChangeEvent<Object, Object>>> destinationEvents : result.entrySet()) {
      numUploadedEvents += this.uploadDestination(destinationEvents.getKey(), destinationEvents.getValue());
    }
    // workaround! somehow offset is not saved to file unless we call committer.markProcessed
    // even its should be saved to file periodically
    for (ChangeEvent<Object, Object> record : records) {
      LOGGER.trace("Processed event '{}'", record);
      committer.markProcessed(record);
    }
    committer.markBatchFinished();
    this.logConsumerProgress(numUploadedEvents);
    LOGGER.debug("Received:{} Processed:{} events", records.size(), numUploadedEvents);

    batchSizeWait.waitMs(records.size(), (int) Duration.between(start, Instant.now()).toMillis());

  }

  protected void logConsumerProgress(long numUploadedEvents) {
    numConsumedEvents += numUploadedEvents;
    if (logTimer.expired()) {
      LOGGER.info("Consumed {} records after {}", numConsumedEvents, Strings.duration(clock.currentTimeInMillis() - consumerStart));
      numConsumedEvents = 0;
      consumerStart = clock.currentTimeInMillis();
      logTimer = Threads.timer(clock, LOG_INTERVAL);
    }
  }

  public abstract long uploadDestination(String destination, List<ChangeEvent<Object, Object>> data) throws InterruptedException;

}
