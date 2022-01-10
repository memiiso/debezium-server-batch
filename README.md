![Java CI with Maven](https://github.com/memiiso/debezium-server-batch/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

# Debezium Batch Consumers

This project adds batch consumers
to [Debezium Server](https://debezium.io/documentation/reference/operations/debezium-server.html). Using batch consumers
its possible to consume CDC events as mini batches

## `sparkbatch` Consumer
Consumes debezium events using spark

@ConfigProperty(name = "debezium.sink.batch.batch-size-wait", defaultValue = "NoBatchSizeWait")
@ConfigProperty(name = "debezium.sink.sparkbatch.bucket-name", defaultValue = "s3a://My-S3-Bucket")
@ConfigProperty(name = "debezium.sink.sparkbatch.save-format", defaultValue = "parquet")
@ConfigProperty(name = "debezium.sink.sparkbatch.save-mode", defaultValue = "append")
@ConfigProperty(name = "debezium.sink.sparkbatch.cast-deleted-field", defaultValue = "false")
@ConfigProperty(name = "debezium.sink.batch.objectkey-partition", defaultValue = "false")
protected Boolean partitionData;
@ConfigProperty(name = "debezium.sink.batch.objectkey-partition-time-zone", defaultValue = "UTC")
protected String partitionDataZone;
@ConfigProperty(name = "debezium.sink.batch.objectkey-prefix", defaultValue = "")
protected Optional<String> objectKeyPrefix;
@ConfigProperty(name = "debezium.sink.batch.destination-regexp", defaultValue = "")
protected Optional<String> destinationRegexp;
@ConfigProperty(name = "debezium.sink.batch.destination-regexp-replace", defaultValue = "")
protected Optional<String> destinationRegexpReplace;

## `bigquerybatch` Consumer
Consumes debezium events to Bigquery using Bigquery writer api.

@ConfigProperty(name = "debezium.sink.batch.batch-size-wait", defaultValue = "NoBatchSizeWait")

@ConfigProperty(name = "debezium.sink.batch.destination-regexp", defaultValue = "")
protected Optional<String> destinationRegexp;
@ConfigProperty(name = "debezium.sink.batch.destination-regexp-replace", defaultValue = "")
protected Optional<String> destinationRegexpReplace;
@Inject
@ConfigProperty(name = "debezium.sink.bigquerybatch.dataset", defaultValue = "")
Optional<String> bqDataset;
@ConfigProperty(name = "debezium.sink.bigquerybatch.location", defaultValue = "US")
String bqLocation;
@ConfigProperty(name = "debezium.sink.bigquerybatch.project", defaultValue = "")
Optional<String> gcpProject;
@ConfigProperty(name = "debezium.sink.bigquerybatch.createDisposition", defaultValue = "CREATE_IF_NEEDED")
String createDisposition;
@ConfigProperty(name = "debezium.sink.bigquerybatch.partitionField", defaultValue = "__source_ts")
String partitionField;
@ConfigProperty(name = "debezium.sink.bigquerybatch.partitionType", defaultValue = "MONTH")
String partitionType;
@ConfigProperty(name = "debezium.sink.bigquerybatch.allowFieldAddition", defaultValue = "true")
Boolean allowFieldAddition;
@ConfigProperty(name = "debezium.sink.bigquerybatch.allowFieldRelaxation", defaultValue = "true")
Boolean allowFieldRelaxation;
@ConfigProperty(name = "debezium.sink.bigquerybatch.credentialsFile", defaultValue = "")
Optional<String> credentialsFile;
@ConfigProperty(name = "debezium.sink.bigquerybatch.cast-deleted-field", defaultValue = "false")
Boolean castDeletedField;

```properties
debezium.format.value.schemas.enable=true
```

### Optimizing batch size (or commit interval)

Debezium extracts database events in real time and this could cause too frequent commits or too many small files
which is not optimal for batch processing especially when near realtime data feed is sufficient.
To avoid this problem following batch-size-wait classes are used.

Batch size wait adds delay between consumer calls to increase total number of events received per call and meanwhile events are collected in memory.
This setting should be configured together with `debezium.source.max.queue.size` and `debezium.source.max.batch.size` debezium properties

#### NoBatchSizeWait

This is default configuration, by default consumer will not use any wait. All the events are consumed immediately.

#### DynamicBatchSizeWait
**Deprecated**
This wait strategy dynamically adds wait to increase batch size. Wait duration is calculated based on number of processed events in
last 3 batches. if last batch sizes are lower than `max.batch.size` Wait duration will increase and if last batch sizes
are bigger than 90% of `max.batch.size` Wait duration will decrease

This strategy optimizes batch size between 85%-90% of the `max.batch.size`, it does not guarantee consistent batch size.

example setup to receive ~2048 events per commit. maximum wait is set to 5 seconds
```properties
debezium.source.max.queue.size=16000
debezium.source.max.batch.size=2048
debezium.sink.batch.batch-size-wait=DynamicBatchSizeWait
debezium.sink.batch.batch-size-wait.max-wait-ms=5000
```
#### MaxBatchSizeWait

MaxBatchSizeWait uses debezium metrics to optimize batch size, this strategy is more precise compared to DynamicBatchSizeWait.
MaxBatchSizeWait periodically reads streaming queue current size and waits until it reaches to `max.batch.size`.
Maximum wait and check intervals are controlled by `debezium.sink.batch.batch-size-wait.max-wait-ms`, `debezium.sink.batch.batch-size-wait.wait-interval-ms` properties.

example setup to receive ~2048 events per commit. maximum wait is set to 30 seconds, streaming queue current size checked every 5 seconds
```properties
debezium.sink.batch.batch-size-wait=MaxBatchSizeWait
debezium.sink.batch.metrics.snapshot-mbean=debezium.postgres:type=connector-metrics,context=snapshot,server=testc
debezium.sink.batch.metrics.streaming-mbean=debezium.postgres:type=connector-metrics,context=streaming,server=testc
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.max.batch.size=2048;
debezium.source.max.queue.size=16000";
debezium.sink.batch.batch-size-wait.max-wait-ms=30000
debezium.sink.batch.batch-size-wait.wait-interval-ms=5000
```

## Flattening Event Data

Batch consumer requires event flattening, please
see [debezium feature](https://debezium.io/documentation/reference/configuration/event-flattening.html#_configuration)

```properties
debezium.transforms=unwrap
debezium.transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
debezium.transforms.unwrap.add.fields=op,table,lsn,source.ts_ms
debezium.transforms.unwrap.add.headers=db
debezium.transforms.unwrap.delete.handling.mode=rewrite
```

## Configuring log levels

```properties
quarkus.log.level=INFO
# Change this to set Spark log level
quarkus.log.category."org.apache.spark".level=WARN
# hadoop, parquet
quarkus.log.category."org.apache.hadoop".level=WARN
quarkus.log.category."org.apache.parquet".level=WARN
# Ignore messages below warning level from Jetty, because it's a bit verbose
quarkus.log.category."org.eclipse.jetty".level=WARN
#
```