/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.adapters.kafka.client.utils;

import java.util.Properties;

public class ConfigMaker {

    public static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

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
        return makeTestProperties(scopeName, controllerUri, groupId, clientId, null, null,
                isProducer);
    }

    public static Properties makeTestProperties(String scopeName, String controllerUri, String groupId,
                                                String clientId, String transactionalId, String transactionTimeoutInMs,
                                                boolean isProducer) {

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
        if (isProducer && transactionalId != null) {
            config.put("transactional.id", transactionalId);
        }
        if (isProducer && transactionalId != null) {
            config.put("transactional.id", transactionalId);
        }
        if (isProducer && transactionTimeoutInMs != null) {
            config.put("transaction.timeout.ms", transactionTimeoutInMs);
        }
        return config;
    }
}
