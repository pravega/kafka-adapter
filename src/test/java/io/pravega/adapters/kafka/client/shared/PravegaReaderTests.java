package io.pravega.adapters.kafka.client.shared;

import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.RequiredArgsConstructor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PravegaReaderTests {

    @Test
    public void readsAreDelegatedToEventReader() {
        EventStreamReader<String> mockInternalReader = mock(EventStreamReader.class);
        when(mockInternalReader.readNextEvent(anyLong())).thenReturn(new FakeEventRead("test-message"));

        PravegaReader<String> reader = dummyReader();
        reader.setReader(mockInternalReader);

        assertEquals("test-message", reader.readNextEvent(100).getEvent());
    }

    private PravegaReader<String> dummyReader() {
        return new PravegaReader<String>("test-scope", "test-stream", "test-uri", new JavaSerializer<String>(),
                "test-rg-name", "test-reader-id");
    }
}

@RequiredArgsConstructor
class FakeEventRead implements EventRead<String> {

    private final String event;

    @Override
    public String getEvent() {
        return this.event;
    }

    @Override
    public Position getPosition() {
        return null;
    }

    @Override
    public EventPointer getEventPointer() {
        return null;
    }

    @Override
    public boolean isCheckpoint() {
        return false;
    }

    @Override
    public java.lang.String getCheckpointName() {
        return null;
    }
}
