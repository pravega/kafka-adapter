package io.pravega.adapters.kafka.client.shared;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;

import java.net.URI;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PravegaReader<T> implements AutoCloseable {

    private final String scope;

    private final String stream;

    private final String controllerUri;

    private final Serializer serializer;

    private final String readerGroupName;
    private final String readerId;

    private EventStreamReader<T> reader;
    private ReaderGroupManager readerGroupManager;

    public PravegaReader(@NonNull String scope, @NonNull String stream, @NonNull String controllerUri,
                         @NonNull Serializer serializer, @NonNull String readerGroupName, @NonNull String readerId) {
        this.scope = scope;
        this.stream = stream;
        this.controllerUri = controllerUri;
        this.serializer = serializer;
        this.readerGroupName = readerGroupName;
        this.readerId = readerId;
    }

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

        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, stream))
                .disableAutomaticCheckpoints()
                .build();

        readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);

        // TODO: Use user-provider reader id
        reader = EventStreamClientFactory.withScope(scope, clientConfig)
                .createReader(readerId, readerGroupName, serializer, ReaderConfig.builder().build());
    }

    public EventRead<T> readNextEvent() {
        if (!isInitialized()) {
            init();
        }
        return this.reader.readNextEvent(200);
    }

    public T tryReadNext() {
        if (!isInitialized()) {
            init();
        }
        EventRead<T> event = this.reader.readNextEvent(200);
        if (event != null) {
            return event.getEvent();

        } else {
            return null;
        }
    }

    public T readNext() {
        T result = tryReadNext();
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
