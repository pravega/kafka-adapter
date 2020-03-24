/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.adapters.kafka.client.config;

import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Properties;

@Slf4j
public abstract class PravegaKafkaConfig {

    @Getter
    protected final PravegaConfig pravegaConfig;

    @Getter(AccessLevel.PROTECTED)
    private final Properties properties;

    public PravegaKafkaConfig(Properties props) {
        if (props.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) == null) {
            throw new IllegalArgumentException(String.format("Property [%s] is not set",
                    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        }
        properties = props;
        pravegaConfig = PravegaConfig.getInstance(props);
    }

    public String evaluateServerEndpoints() {
        if (pravegaConfig.getControllerUri() != null) {
            return pravegaConfig.getControllerUri();
        } else {
            return this.properties.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        }
    }

    public String getScope() {
        return pravegaConfig.getScope();
    }

    public String evaluateGroupId(String defaultValue) {
        return properties.getProperty(CommonClientConfigs.GROUP_ID_CONFIG, defaultValue);
    }

    public String evaluateClientId(String defaultValue) {
        return properties.getProperty(CommonClientConfigs.CLIENT_ID_CONFIG, defaultValue);
    }

    protected Serializer evaluateSerde(@NonNull String key) {
        Object serdeValue = this.properties.get(key);

        if (serdeValue == null) {
            throw new IllegalArgumentException(String.format("No property with name [%s] found", key));
        }

        if (serdeValue instanceof String) {
            return instantiateSerdeFromClassName((String) serdeValue);
        } else if (serdeValue instanceof Serializer) {
            return (Serializer) serdeValue;
        } else if (serdeValue instanceof Class) {
            return instantiateSerdeFromClassName(((Class) serdeValue).getCanonicalName());
        } else {
            throw new IllegalArgumentException(
                    String.format("Could not instantiate Serde from property key [%s]", key));
        }
    }

    private Serializer instantiateSerdeFromClassName(@NonNull String fqClassName) {
        if (fqClassName.equals("org.apache.kafka.common.serialization.StringSerializer") ||
                fqClassName.equals("org.apache.kafka.common.serialization.StringDeserializer")) {
            return new JavaSerializer<String>();
        } else {
            try {
                return (Serializer) Class.forName(fqClassName).newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                log.error("Unable to instantiate serializer with name [{}]", fqClassName, e);
                throw new IllegalStateException(e);
            }
        }
    }
}
