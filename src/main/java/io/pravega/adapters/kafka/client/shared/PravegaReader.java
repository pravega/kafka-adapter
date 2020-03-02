package io.pravega.adapters.kafka.client.shared;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.JavaSerializer;

import java.net.URI;
import java.util.UUID;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
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

    public EventRead<String> readNextEvent() {
        if (!isInitialized()) {
            init();
        }
        return this.reader.readNextEvent(200);
    }

    public String tryReadNext() {
        if (!isInitialized()) {
            init();
        }

        EventRead<String> event = this.reader.readNextEvent(200);

        if (event != null) {
            return event.getEvent();

        } else {
            return null;
        }
    }

    public String readNext() {
        String result = tryReadNext();
        if (result == null) {
            throw new IllegalStateException("No Event");
        }
        return result;
    }

    @Override
    public void close() {
        try {
            reader.close();
            readerGroupManager.close();
        } catch (Exception e) {
            log.warn("Encountered exception in cleaning up", e);
        }
    }
}
