/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.spark;

import io.debezium.DebeziumException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.spark.bigquery.repackaged.com.google.auth.oauth2.GoogleCredentials;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GCSAccessTokenProvider implements AccessTokenProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(GCSAccessTokenProvider.class);
  GoogleCredentials googleCredentials;
  private Configuration config;

  @Override
  public AccessToken getAccessToken() {

    try {
      googleCredentials.refreshIfExpired();
    } catch (IOException e) {
      throw new DebeziumException("Failed to refresh google credentials", e);
    }

    com.google.cloud.spark.bigquery.repackaged.com.google.auth.oauth2.AccessToken token = googleCredentials.getAccessToken();
    return new AccessToken(token.getTokenValue(), token.getExpirationTime().getTime());
  }

  @Override
  public void refresh() throws IOException {
    googleCredentials.refresh();
  }

  @Override
  public Configuration getConf() {
    return this.config;
  }

  @Override
  public void setConf(Configuration config) {

    this.config = config;
    String credentialsFile = config.get("credentialsFile");

    if (credentialsFile == null) {
      throw new DebeziumException("Please provide a value for `debezium.sink.sparkbatch.spark.datasource.bigquery.credentialsFile`");
    }

    File credFile = new File(credentialsFile);

    if (!credFile.isFile() || !credFile.exists()) {
      throw new DebeziumException("Credentials file does not exist.");
    }

    try {
      LOGGER.debug("Initialized GoogleCredentials from file:{}", credentialsFile);
      googleCredentials = GoogleCredentials.fromStream(new FileInputStream(credFile));
    } catch (IOException e) {
      throw new DebeziumException("Failed to initialize Google Credentials", e);
    }

  }
}
