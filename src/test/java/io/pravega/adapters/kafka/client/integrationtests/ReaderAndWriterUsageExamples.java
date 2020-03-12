package io.pravega.adapters.kafka.client.integrationtests;

import io.pravega.adapters.kafka.client.shared.PravegaReader;
import io.pravega.adapters.kafka.client.shared.PravegaWriter;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;

import java.net.URI;
import java.util.Arrays;
import java.util.UUID;

import lombok.Cleanup;
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
            assertEquals("Message - 1", reader.readNext(200));
            assertEquals("Message - 2", reader.readNext(200));
            assertEquals("Message - 3", reader.readNext(200));
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
                System.out.println("Reader read message: " + reader.readNext(200));
            }
        }

        try (PravegaReader reader = new PravegaReader(scope, topic, controllerUri, new JavaSerializer<String>(),
                readerGroupName, readerId2)) {
            for (int i = 0; i < 4; i++) {
                System.out.println("A new reader read message: " + reader.readNext(200));
            }
        }
    }

    @Test
    public void writeToThenReadFromMultipleStreamsUsingSingleGroup() {
        final String scope = "rwna-" + Math.random();
        final String stream1 = "test-stream-1";
        final String stream2 = "test-stream-2";
        final String controllerUri = "tcp://localhost:9090";
        final int numSegments = 1;
        final String writeEvent1 = "This is event 1 in stream 1";
        final String writeEvent2 = "This is event 2 in stream 1";

        // Arrange

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(controllerUri))
                .build();

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        System.out.println("Created a stream manager");

        // Create scope
        streamManager.createScope(scope);
        System.out.format("Created a scope [%s]%n", scope);

        // Create stream 1
        streamManager.createStream(scope, stream1, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numSegments))
                .build());
        System.out.format("Created stream1 with name [%s]%n", stream1);

        // Create stream 2
        streamManager.createStream(scope, stream2, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numSegments))
                .build());
        System.out.format("Created stream2 with name [%s]%n", stream2);

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        @Cleanup
        EventStreamWriter<String> writer1 = clientFactory.createEventWriter(stream1,
                new JavaSerializer<String>(), EventWriterConfig.builder().build());
        writer1.writeEvent(writeEvent1).join();
        System.out.format("Done writing event 1 = [%s] to stream 1 = [%s]%n", writeEvent1, stream1);

        @Cleanup
        EventStreamWriter<String> writer2 = clientFactory.createEventWriter(stream2,
                new JavaSerializer<String>(), EventWriterConfig.builder().build());
        writer2.writeEvent(writeEvent2).join();
        System.out.format("Done writing event 2 = [%s] to stream 2 = [%s]%n", writeEvent2, stream2);

        // Act and assert
        try (PravegaReader<String> reader = new PravegaReader(scope, Arrays.asList(stream1, stream2), controllerUri,
                new JavaSerializer<String>(),
                UUID.randomUUID().toString(), "readerId")) {
            String readEvent1 = reader.readNext(200);
            System.out.format("Read event 1: [%s]%n", readEvent1);
            String readEvent2 = reader.readNext(200);
            System.out.format("Read event 2: [%s]%n", readEvent2);
            assertEquals(writeEvent1, readEvent1);
            assertEquals(writeEvent2, readEvent2);
        }
    }
}
