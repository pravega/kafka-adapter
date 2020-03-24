/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.adapters.kafka.client.shared;

import io.pravega.adapters.kafka.client.utils.ConfigMaker;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PravegaProducerConfigTests {

    @Test(expected = IllegalArgumentException.class)
    public void emptyServerConfigCausesInstantiationFailure() {
        PravegaProducerConfig config = new PravegaProducerConfig(new Properties());
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptySerializerConfigCausesInstantiationFailure() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");
        PravegaProducerConfig config = new PravegaProducerConfig(props);
    }

    @Test
    public void serverEndpointsReturnsPravegaConfigValueFirst() {
        Properties props = new Properties();
        props.setProperty(PravegaConfig.CONTROLLER_URI, "pravega://localhost:9090");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ConfigMaker.STRING_SERIALIZER);

        PravegaProducerConfig config = new PravegaProducerConfig(props);
        assertEquals("pravega://localhost:9090", config.getServerEndpoints());
    }

    @Test
    public void serverEndpointsReturnsKafkaConfigValueIfPravegaSpecificConfigUnspecified() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "tcp://localhost:9090");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ConfigMaker.STRING_SERIALIZER);

        PravegaProducerConfig config = new PravegaProducerConfig(props);
        assertEquals("tcp://localhost:9090", config.getServerEndpoints());
    }

    @Test
    public void returnsEmptyInterceptsIfNoInterceptorsSpecified() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "tcp://localhost:9090");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ConfigMaker.STRING_SERIALIZER);
        PravegaProducerConfig config = new PravegaProducerConfig(props);

        assertNotNull(config.getInterceptors());
    }

    @Test
    public void returnsLoadedProducerInterceptor() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "tcp://localhost:9090");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ConfigMaker.STRING_SERIALIZER);
        props.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.pravega.adapters.kafka.client.utils.FakeKafkaProducerInterceptor");
        PravegaProducerConfig config = new PravegaProducerConfig(props);
        ProducerInterceptors interceptors = config.getInterceptors();
    }
}
