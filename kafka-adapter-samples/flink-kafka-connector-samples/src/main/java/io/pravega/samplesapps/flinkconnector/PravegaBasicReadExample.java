/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.samplesapps.flinkconnector;

import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.samplesapps.shared.PravegaReader;
import io.pravega.samplesapps.shared.PravegaWriter;
import io.pravega.samplesapps.shared.StringDeserializationSchema;
import java.util.List;
import java.util.UUID;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PravegaBasicReadExample extends BasicReadExample {

    private final static String DEFAULT_SCOPE = "migrated-from-kafka";

    public PravegaBasicReadExample(boolean isCreateTestData, @NonNull String bootstrapServer, @NonNull String clientId,
                                   @NonNull String topic) {
        super(isCreateTestData, bootstrapServer, clientId, topic, new StringDeserializationSchema());
    }

    @Override
    protected final void createTestData() {
        writeTestDataToPravega(DEFAULT_SCOPE, this.getStream(), this.getBootstrapServer());
        // readThenPrintDataFromPravega(DEFAULT_SCOPE, this,getStream(), this.bootstrapServer());
    }

    private void writeTestDataToPravega(String scope, String stream, String bootstrapServers) {
        try (PravegaWriter<String> writer = new PravegaWriter<String>(DEFAULT_SCOPE, this.getStream(),
                this.getBootstrapServer(), new JavaSerializer<String>(), 1)) {
            for (int i = 0; i < 10; i++) {
                writer.writeEvent("message " + i).join();
                log.debug("Wrote message {}", i);
            }
            writer.flush();
        }
    }

    private void readThenPrintDataFromPravega(String scope, String stream, String bootstrapServers) {
        try (PravegaReader<String> reader = new PravegaReader<String>(scope, stream, bootstrapServers,
                new JavaSerializer<String>(), UUID.randomUUID().toString(), "readerId")) {
            List<String> results = reader.readAll(5000);
            for (int i = 0; i < results.size(); i++) {
                log.info("Consumed message: {}", results.get(i));
            }
        }
    }

    public static void main(String[] args) {
        PravegaBasicReadExample driver = new PravegaBasicReadExample(false, "tcp://localhost:9090",
                "testClient", "test-topic");
        driver.execute();
    }
}
