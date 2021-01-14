![Java CI with Maven](https://github.com/memiiso/debezium-server-memiiso/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

@TODO make sure mapdb creates different files each time! random!

# Debezium Batch Consumers

This project adds various batch consumers
to [debezium server](https://debezium.io/documentation/reference/operations/debezium-server.html)

## Configuring Debezium Batch size

`max.batch.size` Positive integer value that specifies the maximum size of each batch of events that should be processed
during each iteration of this connector. Defaults to 2048.

`poll.interval.ms` Positive integer value that specifies the number of milliseconds the connector should wait for new
change events to appear before it starts processing a batch of events. Defaults to 1000 milliseconds, or 1 second.

`max.queue.size` Positive integer value that specifies the maximum size of the blocking queue into which change events
read from the database log are placed before they are written to Kafka. This queue can provide backpressure to the
binlog reader when, for example, writes to Kafka are slow or if Kafka is not available. Events that appear in the queue
are not included in the offsets periodically recorded by this connector. Defaults to 8192, and should always be larger
than the maximum batch size specified by the max.batch.size property.

## `s3` Consumer

object names are mapped like

`objectkey.prefix + event.destination + "/" + daily partition of batch time + "/" + random UUID + "." + debezium.format.value{json,avro...};`

```
debezium.sink.type=s3
debezium.sink.s3.region = S3_REGION
debezium.sink.s3.bucket.name = s3a://S3_BUCKET
debezium.sink.s3.endpointoverride = http://localhost:9000, default:'false'
debezium.sink.s3.credentials.profile = default:'default'
debezium.sink.s3.credentials.useinstancecred = false
debezium.sink.s3.objectkey.prefix = debezium-cdc-
```

## `batch` Consumer

Batch consumer is based on json events! `debezium.format.value=json`

collects debezium events @TODO add more detail, ex: mapdb

##### Common Batch Consumer Parameters

```
debezium.sink.batch.row.limit = 2 # number of rows to triger data upload per event.destination(table)
debezium.sink.batch.time.limit = 2 # DISABLED seconds interval to trigger data upload
debezium.sink.batch.objectkey.prefix = debezium-cdc-
io.debezium.server.batch.batchwriter.BatchRecordWriter = # one of {s3json,spark,sparkdelta,sparkiceberg} details explained below
io.debezium.server.batch.keymapper.ObjectKeyMapper = # one of {dailypartitioned,table,default}
```

### Batch Record Writers

collects json events in local [mapdb](https://github.com/jankotek/mapdb) and writes batch of events periodically to
destination. write/upload is triggered based on debezium batch size and number of events collected for a
destination `debezium.sink.batch.row.limit`

##### s3json

periodically writes events as jsonlines file to s3

Custom parameters are

```
debezium.sink.batch.s3.region = eu-central-1
debezium.sink.batch.s3.endpointoverride = http://localhost:9000, default:'false'
debezium.sink.batch.s3.bucket.name = My-S3-Bucket
debezium.sink.s3.credentials.profile = default, default:'default'
debezium.sink.batch.s3.credentials.useinstancecred = false 
```

a recommended object key mappers are

- `default`
- `dailypartitioned`

##### spark

```
debezium.sink.sparkbatch.saveformat = {json,avro,parquet}
debezium.sink.sparkbatch.removeschema = false, remove schema from event if exists
debezium.sink.sparkbatch.bucket.name = My-S3-Bucket
debezium.sink.sparkbatch.{spark.prop.param} = xyz-value # passed to spark conf!
```

the recommended object key mappers are

- `default`
- `dailypartitioned`

##### sparkiceberg

appends data to iceberg table if Schema is enabled(`debezium.format.value.schemas.enable`) in data it will use this to
determine data schema otherwise it fallback to schema inference

```
debezium.sink.sparkbatch.saveformat = iceberg
debezium.sink.sparkbatch.removeschema = true, remove schema from event if exists # @TODO
debezium.sink.sparkbatch.bucket.name = My-S3-Bucket
debezium.sink.sparkbatch.{spark.prop.param} = xyz-value # passed to spark conf!
```

the recommended object key mappers are

- `table`

##### sparkdelta

WIP

```
debezium.sink.sparkbatch.saveformat = {json,avro,parquet}
debezium.sink.sparkbatch.removeschema = false, remove schema from event if exists
debezium.sink.sparkbatch.bucket.name = My-S3-Bucket
debezium.sink.sparkbatch.{spark.prop.param} = xyz-value # passed to spark conf!
```

the recommended object key mappers are

- `table`

## `iceberg` Consumer

appends json events to destination iceberg tables, batch size is determined by debezium

```
debezium.sink.iceberg.{iceberg.prop.name} = xyz-value # passed to iceberg!
```

## `icebergevents` Consumer

WIP appends json events to single iceberg table

iceberg table is

```java
static final String TABLE_NAME="debezium_events";
static final Schema TABLE_SCHEMA=new Schema(
        required(1,"event_destination",Types.StringType.get(),"event destination"),
        optional(2,"event_key",Types.StringType.get()),
        optional(3,"event_key_value",Types.StringType.get()),
        optional(4,"event_value",Types.StringType.get()),
        optional(5,"event_value_format",Types.StringType.get()),
        optional(6,"event_key_format",Types.StringType.get()),
        optional(7,"event_sink_timestamp",Types.TimestampType.withZone()));
```