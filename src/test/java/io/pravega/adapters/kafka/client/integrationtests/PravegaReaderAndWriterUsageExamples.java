package io.pravega.adapters.kafka.client.integrationtests;

import io.pravega.adapters.kafka.client.shared.PravegaReader;
import io.pravega.adapters.kafka.client.shared.PravegaWriter;
import io.pravega.client.stream.impl.JavaSerializer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PravegaReaderAndWriterUsageExamples {

    @Test
    public void writerAndReaderExample() {
        String scope = "test-scope";
        String topic = "test-stream-" + Math.random();
        String controllerUri = "tcp://localhost:9090";

        try (PravegaWriter<String> writer = new PravegaWriter(scope, topic, controllerUri,
                new JavaSerializer<String>())) {
            writer.writeEvent("Message - 1")
                    .thenRun(() -> writer.writeEvent("Message - 2"))
                    .thenRun(() -> writer.writeEvent("Message - 3"))
                    .join();
        }

        try (PravegaReader reader = new PravegaReader(scope, topic, controllerUri, new JavaSerializer<String>())) {
            assertEquals("Message - 1", reader.readNext());
            assertEquals("Message - 2", reader.readNext());
            assertEquals("Message - 3", reader.readNext());
        }
    }
}
