package io.pravega.adapters.kafka.client.shared;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class PravegaWriter<T> implements AutoCloseable {

    @NonNull
    private final String scope;

    @NonNull
    private final String stream;

    @NonNull
    private final String controllerUri;

    @NonNull
    private final Serializer<T> serializer;

    private StreamManager streamManager;
    private EventStreamClientFactory clientFactory;
    private EventStreamWriter<T> writer;

    private boolean isInitialized() {
        return writer != null;
    }

    private boolean isClosed = false;

    public void init() {
        if (isInitialized()) {
            log.debug("Already initialized");
            return;
        }
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(controllerUri))
                .build();

        streamManager = StreamManager.create(clientConfig);

        boolean isScopeCreated = streamManager.createScope(scope);
        if (isScopeCreated) {
            log.info("Created scope {}", scope);
        } else {
            log.debug("Scope {} was already created previously", scope);
        }

        boolean isStreamCreated = streamManager.createStream(scope, stream, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());

        if (isStreamCreated) {
            log.info("Created stream {} in scope {}", stream, scope);
        } else {
            log.debug("Stream {} in scope {} was already created previously {}", stream, scope);
        }

        clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        log.debug("Creating a writer for scope/stream: {}/{}", scope, stream);
    }

    public CompletableFuture<Void> writeEvent(T event) {
        if (isClosed) {
            throw new IllegalStateException("Already closed");
        }

        if (!isInitialized()) {
            log.info("Not initialized already, initializing");
            this.init();
        }
        return writer.writeEvent(event);
    }

    public void flush() {
        if (isClosed) {
            return;
        }
        log.info("Flushing...");
        writer.flush();
    }

    @Override
    public void close() {
        isClosed = true;
        if (!isInitialized()) {
            return;
        }
        log.debug("Closing...");
        writer.close();
        clientFactory.close();
        streamManager.close();
    }
}
