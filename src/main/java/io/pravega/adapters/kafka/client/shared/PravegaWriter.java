package io.pravega.adapters.kafka.client.shared;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.RequiredArgsConstructor;

import java.net.URI;

@RequiredArgsConstructor
public class PravegaWriter implements AutoCloseable {

    private final String scope;

    private final String stream;

    private final String controllerUri;

    private StreamManager streamManager;
    private EventStreamClientFactory clientFactory;
    private EventStreamWriter<String> writer;

    private boolean isInitialized() {
        return writer != null;
    }

    public void init() {
        if (isInitialized()) {
            return;
        }
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(controllerUri))
                .build();

        streamManager = StreamManager.create(clientConfig);
        streamManager.createScope(scope);

        streamManager.createStream(scope, stream, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());

        clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        writer = clientFactory.createEventWriter(stream,
                new JavaSerializer<String>(),
                EventWriterConfig.builder().build());
    }

    public void writeEvent(String event) {
        if (!isInitialized()) {
            this.init();
        }
        writer.writeEvent(event).join();
    }

    @Override
    public void close() {
        writer.close();
        clientFactory.close();
        streamManager.close();

    }
}
