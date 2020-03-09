package io.pravega.adapters.kafka.client.integrationtests;

import io.pravega.adapters.kafka.client.shared.PravegaReader;
import io.pravega.adapters.kafka.client.shared.PravegaWriter;
import io.pravega.client.stream.impl.JavaSerializer;
import java.util.UUID;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ReaderAndWriterUsageExamples {

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

        try (PravegaReader reader = new PravegaReader(scope, topic, controllerUri, new JavaSerializer<String>(),
                UUID.randomUUID().toString(), "readerId")) {
            assertEquals("Message - 1", reader.readNext());
            assertEquals("Message - 2", reader.readNext());
            assertEquals("Message - 3", reader.readNext());
        }
    }

    @Test
    public void readStartsFromLastPositionIfReaderGroupIsSame() {
        String scope = "test-scope";
        String topic = "test-stream-" + Math.random();
        String controllerUri = "tcp://localhost:9090";

        try (PravegaWriter<String> writer = new PravegaWriter(scope, topic, controllerUri,
                new JavaSerializer<String>())) {
            for (int i = 0; i < 10; i++) {
                String message = "Message: " + i;
                writer.writeEvent(message).join();
                System.out.println("Wrote message: " + message);
            }
        }
        // If we didn't use the same readerGroupName for both the readers, reader 2 would start from the first event,
        // even though reader 1 has already read the first 5 events.
        String readerGroupName = UUID.randomUUID().toString();

        // Even if you had the same readerId for both, reader 2 would have started from the same position where
        // reader 1 leaves, as the reader group name is the same.
        String readerId1 = "reader1";
        String readerId2 = "reader2";
        try (PravegaReader reader = new PravegaReader(scope, topic, controllerUri, new JavaSerializer<String>(),
                    readerGroupName, readerId1)) {
            for (int i = 0; i < 4; i++) {
                System.out.println("Reader read message: " + reader.readNext());
            }
        }

        try (PravegaReader reader = new PravegaReader(scope, topic, controllerUri, new JavaSerializer<String>(),
                readerGroupName, readerId2)) {
            for (int i = 0; i < 4; i++) {
                System.out.println("A new reader read message: " + reader.readNext());
            }
        }
    }
}
