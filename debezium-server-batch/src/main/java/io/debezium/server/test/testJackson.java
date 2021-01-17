/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.test;

import io.debezium.server.batch.JsonDeserializer;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class testJackson {
  protected static final Logger LOGGER = LoggerFactory.getLogger(testJackson.class);

  public static void main(String[] args) throws IOException {
    String data = "{\"id\": 174346346363776, \"itemName\": \"theItem\",\"isCreated\": true,\"createdBy\": 123 }";
    //JsonNode itemWithOwner = new ObjectMapper().readTree(data);
    //System.out.println(itemWithOwner.toString());
    //System.out.println(itemWithOwner.get("createdBy").getNodeType().name());
    //System.out.println(itemWithOwner.get("id").getNodeType().name());

    System.out.println("-------------");
    TypeReference ref = new TypeReference<Map<String, Object>>() {
    };
    JsonDeserializer ds = new JsonDeserializer();
    JsonNode jdondata = ds.deserialize("", data.getBytes());
    com.fasterxml.jackson.databind.ObjectMapper jsonObjectMapper = new ObjectMapper();
    Map<String, Object> mappedResult = jsonObjectMapper.convertValue(jdondata, new TypeReference<Map<String, Object>>() {
    });

    System.out.println(mappedResult);
    for (Map.Entry<String, Object> entry : mappedResult.entrySet()) {
      if (entry.getValue() == null)
        System.out.println("Key : " + entry.getKey() + ", Value : " + entry.getValue() + ", Type : null");
      else
        System.out.println("Key : " + entry.getKey() + ", Value : " + entry.getValue() + ", Type : " + entry.getValue().getClass().getSimpleName());
    }
  }

}
