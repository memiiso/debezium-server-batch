/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.batch.manualtests;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class testJackson {
  private static final Logger LOG = LoggerFactory.getLogger(testJackson.class);

  public static void main(String[] args) throws IOException {
    String data =
        "{\"id\": 174346346363776, \"itemName\": \"theItem\",\"isCreated\": true,\"createdBy\": 123 }";

    System.out.println("-------------");
    TypeReference ref = new TypeReference<Map<String, Object>>() {};
    JsonNode jdondata = new ObjectMapper().readTree(data);
    com.fasterxml.jackson.databind.ObjectMapper jsonObjectMapper = new ObjectMapper();
    Map<String, Object> mappedResult =
        jsonObjectMapper.convertValue(jdondata, new TypeReference<Map<String, Object>>() {});

    System.out.println(mappedResult);
    for (Map.Entry<String, Object> entry : mappedResult.entrySet()) {
      if (entry.getValue() == null)
        System.out.println(
            "Key : " + entry.getKey() + ", Value : " + entry.getValue() + ", Type : null");
      else
        System.out.println(
            "Key : "
                + entry.getKey()
                + ", Value : "
                + entry.getValue()
                + ", Type : "
                + entry.getValue().getClass().getSimpleName());
    }
  }
}
