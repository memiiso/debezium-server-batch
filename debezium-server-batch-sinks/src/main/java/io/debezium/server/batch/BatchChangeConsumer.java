/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch;

import javax.enterprise.context.Dependent;
import javax.inject.Named;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("batch")
@Dependent
@Deprecated
public class BatchChangeConsumer extends BatchSparkChangeConsumerV2 {
}
