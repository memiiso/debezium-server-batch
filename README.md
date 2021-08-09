![Java CI with Maven](https://github.com/memiiso/debezium-server-batch/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

# Debezium Batch Consumers

This project adds batch consumers
to [Debezium Server](https://debezium.io/documentation/reference/operations/debezium-server.html). Using batch consumers
its possible to consume CDC events as mini batches

## Configuring Debezium Batch size

`max.batch.size` Positive integer value that specifies the maximum size of each batch of events that should be processed
during each iteration of this connector. Defaults to 2048.

`poll.interval.ms` Positive integer value that specifies the number of milliseconds the connector should wait for new
change events to appear before it starts processing a batch of events. Defaults to 1000 milliseconds, or 1 second.

## `batch` Consumer

Batch consumer is based on json events! `debezium.format.value=json` batch size and interval is defined by
Debezium `max.batch.size` and `poll.interval.ms`

an example `batch` consumer confuquration:

```properties
debezium.sink.type=sparkbatch
quarkus.arc.selected-alternatives=SparkWriter
debezium.source.max.batch.size=5000
debezium.source.poll.interval.ms=600000
```

### Batch Consumer Writers

batch consumer uses one of `SparkWriter`,`S3JsonWriter`,`SparkIcebergWriter` writer classes to upload/process batch
data. Default is `SparkWriter`
its possible to change/override default writer class with `quarkus.arc.selected-alternatives`

```properties
quarkus.arc.selected-alternatives=S3JsonWriter
```

##### SparkWriter

this writer is using spark to process/upload batch data to destination. If event schema enabled it uses event schema to
read data with schema definition

```
debezium.sink.type=sparkbatch
quarkus.arc.selected-alternatives=SparkWriter
debezium.source.max.batch.size=5000
debezium.source.poll.interval.ms=600000
debezium.sink.sparkbatch.save-format = parquet
debezium.sink.sparkbatch.bucket-name = My-S3-Bucket
debezium.sink.sparkbatch.{spark.prop.param} = xyz-value
```

configurations are

| Property | Default | Description  |
|---|---|---|
|`debezium.sink.sparkbatch.save-format` | `parquet` |  Spark dataframe save format  |
|`debezium.sink.sparkbatch.save-mode` | `append` | Spark dataframe save mode  |
|`debezium.sink.sparkbatch.bucket-name` | `s3a://My-S3-Bucket` | Destination bucket |
|`debezium.sink.sparkbatch.{spark.prop.param}` | `xyz` | Spark Configuration parameter, Passed to spark conf |
|`debezium.sink.batch.objectkey-partition` | `false` | Partition data based on event writing/processing time |
|`debezium.sink.batch.objectkey-prefix`| | Prefix to append to object names for example: using `debezium.sink.batch.objectkey-prefix=rawdata/debezium-` will change destination for `test.inventory.customers` -> to `rawdata/debezium-test.inventory.customers` |

##### S3JsonWriter

Uploads events as jsonlines to s3

Custom parameters are

```
debezium.sink.type=sparkbatch
quarkus.arc.selected-alternatives=S3JsonWriter
debezium.sink.batch.s3.region = eu-central-1
debezium.sink.batch.s3.endpoint-override = http://localhost:9000, default:'false'
debezium.sink.batch.s3.bucket-name = My-S3-Bucket
debezium.sink.batch.s3.credentials-use-instance-cred = false 
```

| Property | Default | Description  |
|---|---|---|
|`debezium.sink.batch.s3.region` | `eu-central-1` |  S3 region  |
|`debezium.sink.batch.s3.endpoint-override` | `false` | when its set the value used to override S3 endpoint(default `false`), for example for using minio `http://localhost:9000` |
|`debezium.sink.sparkbatch.bucket-name` | `My-S3-Bucket` | Destination bucket |
|`debezium.sink.batch.s3.credentials-use-instance-cred` | `false` | If set to true uses instance credentials to write s3 |
|`debezium.sink.batch.objectkey-partition` | `false` | Partition data based on event writing/processing time |
|`debezium.sink.batch.objectkey-prefix`| | Prefix to append to object names for example: using `debezium.sink.batch.objectkey-prefix=rawdata/debezium-` will change destination for `test.inventory.customers` -> to `rawdata/debezium-test.inventory.customers` |

##### SparkIcebergWriter (WIP)

WIP - Consumer

## `cachedbatch` Consumer

Batch consumer collects json events in a local cache (Default [Infinispan](https://infinispan.org/)) and writes a batch
of events periodically to destination. batch size and interval controlled by `debezium.sink.batch.row-limit`
and `debezium.sink.batch.time-limit`
with this consumer data consuming and processing is asynchronous. Consumer uses thread pool to parallelize
uploads/writes. A batch upload triggered either by periodic time interval(`debezium.sink.batch.time-limit`), or the
number of rows collected (`debezium.sink.batch.row-limit`) in the cache for a destination(table).

Available cache implementations are `InfinispanCache` and `MemoryCache`, InfinispanCache persist data to disk
MemoryCache uses ConcurrentHashMap and keeps data in memory

an example `batch` consumer configuration:

```properties
debezium.sink.type=cachedbatch
quarkus.arc.selected-alternatives=SparkWriter,InfinispanCache
debezium.source.max.batch.size=5000
debezium.source.poll.interval.ms=60000 # 1 minute
debezium.sink.batch.row-limit=100000 # batch size
debezium.sink.batch.time-limit=600 # 600 seconds / 10 minutes
debezium.sink.batch.cache.store=simple # use infinispan simple cache store 
```

| Property | Default | Description  |
|---|---|---|
|`quarkus.arc.selected-alternatives` | `SparkWriter,InfinispanCache`| Used to choose cache implementation and Writer class implementation  |
|`debezium.sink.batch.row-limit` | `10000`| Max row limit per destination(table) to trigger batch upload and maximum row limit of each batch |
|`debezium.sink.batch.time-limit` | `600`| Time interval to trigger data upload / batch processing  |
|`debezium.sink.batch.destination-upload-threads` | `1`| Number of parallel Writer threads per destination(table) |

#### `InfinispanCache` Cache

Uses [Infinispan](https://infinispan.org/) as local Cache and persists data to disk, supports 3 Infinispan cache
types `simple`(Default), `local`, `rocksdb`

Available Infinispan configurations are

| Property | Default | Description  |
|---|---|---|
|`debezium.sink.batch.cache.store` | `local`| Infinispan Cache Store `simple`(Default), `local`, `rocksdb`  |
|`debezium.sink.batch.cache.location` | `cache`| File location of the cache store  |
|`debezium.sink.batch.cache.memory-maxcount` | `-1`| Controls the memory storage configuration for the cache, Number of entries to keep in memeory  |
|`debezium.sink.batch.cache.max-batch-size` | `1024`| Sets the maximum size of a batch to insert of delete from the cache store |
|`debezium.sink.batch.cache.purge-on-startup` | `false`| Purge cache on startup |
|`debezium.sink.batch.cache.unreliable-return-values` | `false`| Specify whether Infinispan is allowed to disregard the Map contract when providing return values for BasicCache.put(Object, Object) and BasicCache.remove(Object) methods. |

#### `MemoryCache` Cache

MemoryCache uses ConcurrentHashMap and keeps data in memory.

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

## Enabling schema

By some writer implementations(ex:SparkWriter) event schema use used to determine the batch data schema. When enabled,
spark reads batch of data using the schema information form an event. With this option spark data format determined
consistently otherwise Spark infers the schema automatically.

```properties
debezium.format.value.schemas.enable=true
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