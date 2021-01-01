![Java CI with Maven](https://github.com/memiiso/debezium-server-memiiso/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

# Various Debezium Consumers

## `s3` Consumer

object names are mapped like

objectKeyPrefix + destination + "/" + time_daily_partiton + "/" + file_name;

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

all the batch consumers are only supporting json events! `debezium.format.value=json`

##### Common Batch Consumer Parameters

```
debezium.sink.batch.row.limit = 2 # number of rows to triger data upload
debezium.sink.batch.time.limit = 2 # seconds interval to trigger data upload
debezium.sink.batch.objectkey.prefix = debezium-cdc-
io.debezium.server.batch.batchwriter.BatchRecordWriter = # one of {io.debezium.server.batch.batchwriter.SparkBatchRecordWriter,io.debezium.server.batch.batchwriter.SparkDeltaBatchRecordWriter,io.debezium.server.batch.batchwriter.SparkIcebergBatchRecordWriter,io.debezium.server.batch.batchwriter.S3JsonBatchRecordWriter}
io.debezium.server.batch.keymapper.ObjectKeyMapper = # one of {io.debezium.server.batch.keymapper.DefaultObjectKeyMapper, io.debezium.server.batch.keymapper.TimeBasedDailyObjectKeyMapper, io.debezium.server.batch.keymapper.LakeTableObjectKeyMapper}
```

### Batch Record Writers

##### io.debezium.server.batch.batchwriter.S3JsonBatchRecordWriter

periodically writes events as jsonlines file to s3 Custom parameters

```
debezium.sink.batch.s3.region = eu-central-1
debezium.sink.batch.s3.endpointoverride = http://localhost:9000, default:'false'
debezium.sink.batch.s3.bucket.name = My-S3-Bucket
debezium.sink.s3.credentials.profile = default, default:'default'
debezium.sink.batch.s3.credentials.useinstancecred = false 
```

recommended object key mappers are

- `io.debezium.server.batch.keymapper.DefaultObjectKeyMapper`
- `io.debezium.server.batch.keymapper.TimeBasedDailyObjectKeyMapper`

##### io.debezium.server.batch.batchwriter.SparkBatchRecordWriter

```
debezium.sink.sparkbatch.saveformat = {json,avro,parquet}
debezium.sink.sparkbatch.removeschema = false, remove schema from event if exists
debezium.sink.sparkbatch.bucket.name = My-S3-Bucket
debezium.sink.sparkbatch.{spark.prop.param} = xyz-value # passed to spark conf!
io.debezium.server.batch.keymapper.ObjectKeyMapper = {io.debezium.server.batch.keymapper.DefaultObjectKeyMapper, io.debezium.server.batch.keymapper.TimeBasedDailyObjectKeyMapper, io.debezium.server.batch.keymapper.LakeTableObjectKeyMapper}
```

the recommended object key mappers are

- `io.debezium.server.batch.keymapper.DefaultObjectKeyMapper`
- `io.debezium.server.batch.keymapper.TimeBasedDailyObjectKeyMapper`

##### io.debezium.server.batch.batchwriter.SparkIcebergBatchRecordWriter

```
debezium.sink.sparkbatch.saveformat = {json,avro,parquet}
debezium.sink.sparkbatch.removeschema = false, remove schema from event if exists
debezium.sink.sparkbatch.bucket.name = My-S3-Bucket
debezium.sink.sparkbatch.{spark.prop.param} = xyz-value # passed to spark conf!
io.debezium.server.batch.keymapper.ObjectKeyMapper = io.debezium.server.batch.keymapper.LakeTableObjectKeyMapper
```

the recommended object key mappers are

- `io.debezium.server.batch.keymapper.LakeTableObjectKeyMapper`

## `iceberg` Consumer

WIP
