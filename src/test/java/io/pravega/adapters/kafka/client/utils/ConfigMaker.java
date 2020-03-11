package io.pravega.adapters.kafka.client.utils;

import java.util.Properties;

public class ConfigMaker {

    public static Properties makeProducerProperties(String scopeName, String controllerUri, String groupId,
                                             String clientId) {
        return makeTestProperties(scopeName, controllerUri, groupId, clientId, true);
    }

    public static Properties makeConsumerProperties(String scopeName, String controllerUri, String groupId,
                                             String clientId) {
        return makeTestProperties(scopeName, controllerUri, groupId, clientId, false);
    }

    public static Properties makeTestProperties(String scopeName, String controllerUri, String groupId,
                                                 String clientId, boolean isProducer) {

        Properties config = new Properties();
        config.put("bootstrap.servers", controllerUri);
        if (groupId != null) {
            config.put("group.id", groupId);
        }
        if (clientId != null) {
            config.put("client.id", clientId);
        }
        if (isProducer) {
            config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        } else {
            config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        }
        if (scopeName != null) {
            config.put("pravega.scope", scopeName);
        }

        return config;
    }
}
