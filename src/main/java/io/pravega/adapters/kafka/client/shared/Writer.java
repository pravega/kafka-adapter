package io.pravega.adapters.kafka.client.shared;

import java.util.concurrent.CompletableFuture;

public interface Writer<T> extends AutoCloseable {

    CompletableFuture<Void> writeEvent(T event);

    void flush();

    void init();
}
