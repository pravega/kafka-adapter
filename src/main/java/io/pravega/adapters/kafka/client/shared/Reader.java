package io.pravega.adapters.kafka.client.shared;

import io.pravega.client.stream.EventRead;

import java.util.List;

public interface Reader<T> extends AutoCloseable {

    List<T> readAll(long timeoutInMillis);

    EventRead<T> readNextEvent(long timeoutInMillis);

    T tryReadNext(long timeinMillis);

    T readNext(long timeinMillis);

}
