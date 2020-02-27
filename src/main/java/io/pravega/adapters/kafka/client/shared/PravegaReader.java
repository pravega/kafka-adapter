package io.pravega.adapters.kafka.client.shared;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.RequiredArgsConstructor;

import java.net.URI;
import java.util.UUID;

@RequiredArgsConstructor
public class PravegaReader implements AutoCloseable {

    private final String scope;

    private final String stream;

    private final String controllerUri;

    private EventStreamReader<String> reader;
    private ReaderGroupManager readerGroupManager;

    private boolean isInitialized() {
        return reader != null;
    }

    public void init() {
        if (isInitialized()) {
            return;
        }
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(controllerUri))
                .build();

        String readerGroupName = UUID.randomUUID().toString().replace("-", "");
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, stream))
                .disableAutomaticCheckpoints()
                .build();

        readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);

        reader = EventStreamClientFactory.withScope(scope, clientConfig)
                .createReader("readerId", readerGroupName,
                        new JavaSerializer<String>(), ReaderConfig.builder().build());
    }

    public String readNext() {
        if (!isInitialized()) {
            init();
        }

        EventRead<String> event = this.reader.readNextEvent(2000);
        if (event != null) {
            return this.reader.readNextEvent(2000).getEvent();
        } else {
            throw new IllegalStateException("No Event");
        }
    }

    @Override
    public void close() {
        reader.close();
        readerGroupManager.close();
    }
}
