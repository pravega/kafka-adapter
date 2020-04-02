/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.adapters.kafka.client.testutils;

public class ConfigExtractor {

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "tcp://localhost:9090";
    private static final String DEFAULT_SECURE_BOOTSTRAP_SERVERS = "tls://localhost:9090";

    public static String extractBootstrapServers() {
        return extractBootstrapServers(false);
    }

    public static String extractBootstrapServers(boolean securityEnabled) {
        String result = System.getProperty("bootstrap.servers",
                securityEnabled ? DEFAULT_SECURE_BOOTSTRAP_SERVERS : DEFAULT_BOOTSTRAP_SERVERS);
        if (result == null || result.trim().equals("")) {
            throw new IllegalStateException();
        }
        return result;
    }
}
