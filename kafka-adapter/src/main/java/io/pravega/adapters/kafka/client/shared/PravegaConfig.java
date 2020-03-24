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

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

@Slf4j
@Builder
public class PravegaConfig {

    // Property keys
    public static final String CONTROLLER_URI = "pravega.controller.uri";
    public static final String SCOPE = "pravega.scope";
    public static final String NUM_SEGMENTS = "pravega.segments.count";
    public static final String READ_TIMEOUT = "pravega.read.timeout.ms";

    // Values
    public static final String DEFAULT_SCOPE = "migrated-from-kafka";
    public static final int DEFAULT_READ_TIMEOUT = 200; // in milliseconds

    @Getter
    private final String scope;

    @Getter
    private final String controllerUri;

    @Getter
    private final int numSegments;

    @Getter
    private final int readTimeoutInMs;

    public static PravegaConfig getInstance(@NonNull Properties properties) {
        PravegaConfigBuilder builder = PravegaConfig.builder();

        if (properties.getProperty(CONTROLLER_URI) != null) {
            builder.controllerUri = properties.getProperty(CONTROLLER_URI);
        }

        if (properties.getProperty(SCOPE) != null) {
            builder.scope = properties.getProperty(SCOPE);
        }
        builder.numSegments = numSegments(properties);
        builder.readTimeoutInMs = readTimeout(properties);
        return builder.build();
    }

    private static int numSegments(Properties properties) {
        String value = properties.getProperty(NUM_SEGMENTS);

        if (value == null) {
            return -1;
        }

        if (value.matches("\\d+")) {
            return Integer.parseInt(value);
        } else {
            return -1;
        }
    }

    public static int readTimeout(Properties properties) {
        String timeout = properties.getProperty(READ_TIMEOUT);

        if (timeout != null && timeout.matches("\\d+")) {
            return Integer.parseInt(timeout);
        } else {
            return DEFAULT_READ_TIMEOUT;
        }
    }

}
