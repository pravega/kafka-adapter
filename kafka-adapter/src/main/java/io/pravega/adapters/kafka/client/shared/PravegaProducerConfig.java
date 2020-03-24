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

import io.pravega.client.stream.Serializer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
public class PravegaProducerConfig extends PravegaKafkaConfig {

    @Getter
    private final Serializer serializer;

    @Getter
    private final int numSegments;

    @Getter
    private ProducerInterceptors interceptors;

    public PravegaProducerConfig(Properties props) {
        super(props);
        serializer = this.instantiateSerde(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        numSegments = this.getPravegaConfig().getNumSegments();
        interceptors = instantiateInterceptors(props);
    }

    private static <K, V> ProducerInterceptors<K, V> instantiateInterceptors(Properties properties) {
        List<ProducerInterceptor<K, V>> ic = new ArrayList<ProducerInterceptor<K, V>>();
        ProducerInterceptors result = new ProducerInterceptors<K, V>(ic);

        String producerInterceptorClass = properties.getProperty(
                org.apache.kafka.clients.producer.ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);

        if (producerInterceptorClass != null) {
            try {
                ProducerInterceptor<K, V> interceptor =
                        (ProducerInterceptor) Class.forName(producerInterceptorClass).newInstance();
                ic.add(interceptor);
                log.debug("Adding interceptor [{}] to the producer interceptor list", interceptor);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                log.error("Unable to instantiate producer interceptor with name [{}]", producerInterceptorClass, e);
                throw new IllegalStateException(e);
            }
        }
        return result;
    }
}
