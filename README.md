![Java CI with Maven](https://github.com/memiiso/debezium-server-batch/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

# Debezium Batch Consumers

This project adds batch consumer(and local cache)
to [Debezium Server](https://debezium.io/documentation/reference/operations/debezium-server.html)

## Configuring Debezium Batch size

`max.batch.size` Positive integer value that specifies the maximum size of each batch of events that should be processed
during each iteration of this connector. Defaults to 2048.

`poll.interval.ms` Positive integer value that specifies the number of milliseconds the connector should wait for new
change events to appear before it starts processing a batch of events. Defaults to 1000 milliseconds, or 1 second.

## `batch` Consumer

Batch consumer is based on json events! `debezium.format.value=json`
Batch consumer collects json events in local [Infinispan](https://infinispan.org/) cache and writes batch of events
periodically to destination.

##### Common Batch Consumer Parameters

```
debezium.sink.batch.row-limit = 2 # number of rows to triger data upload per event.destination(table)
debezium.sink.batch.time-limit = 2 # seconds interval to trigger data upload
debezium.sink.batch.objectkey-prefix = debezium-cdc-
debezium.sink.batch.objectkey-partition = true
```

### Batch Consumer Types

Record writes are uploading local cached batch of events periodically to destination. write/upload is triggered based on
either total number of events collected for a destination `debezium.sink.batch.row-limit`
or time limit `debezium.sink.batch.time-limit`
##### s3json

Periodically uploads events as jsonlines to s3

Custom parameters are

```
debezium.sink.type=batch
debezium.sink.batch.writer=s3json
debezium.sink.batch.s3.region = eu-central-1
debezium.sink.batch.s3.endpoint-override = http://localhost:9000, default:'false'
debezium.sink.batch.s3.bucket-name = My-S3-Bucket
debezium.sink.batch.s3.credentials.profile = default, default:'default'
debezium.sink.batch.s3.credentials.use-instance-cred = false 
```

##### spark

Periodically uploads events using spark

```
debezium.sink.type=batch
debezium.sink.batch.writer=spark
debezium.sink.sparkbatch.save-format = {json,avro,parquet}
debezium.sink.sparkbatch.bucket-name = My-S3-Bucket
debezium.sink.sparkbatch.{spark.prop.param} = xyz-value # passed to spark conf!
```

##### sparkiceberg - WIP

## `s3` Consumer

Writes each event to s3 to an individual file.

```
debezium.sink.type=s3
debezium.sink.s3.region = S3_REGION
debezium.sink.s3.bucket-name = s3a://S3_BUCKET
debezium.sink.s3.endpoint-override = http://localhost:9000, default:'false'
debezium.sink.s3.credentials.profile = default:'default'
debezium.sink.s3.credentials.use-instance-cred = false
debezium.sink.s3.objectkey-prefix = debezium/cdc-
```