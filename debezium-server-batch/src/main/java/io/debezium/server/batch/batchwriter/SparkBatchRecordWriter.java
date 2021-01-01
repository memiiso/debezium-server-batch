/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.batch.batchwriter;

import java.net.URISyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.server.batch.keymapper.ObjectKeyMapper;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
public class SparkBatchRecordWriter extends AbstractSparkBatchRecordWriter {

    protected static final Logger LOGGER = LoggerFactory.getLogger(SparkBatchRecordWriter.class);

    public SparkBatchRecordWriter(ObjectKeyMapper mapper) throws URISyntaxException {
        super(mapper);
        LOGGER.info("Starting S3 Spark Consumer({})", this.getClass().getName());
        LOGGER.info("Spark save format is '{}'", saveFormat);
    }
}
