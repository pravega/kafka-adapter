package io.pravega.adapters.kafka.client.shared;

import io.pravega.client.stream.impl.JavaSerializer;
import org.junit.Test;

import static io.pravega.adapters.kafka.client.utils.TestUtils.assertThrows;

public class PravegaWriterTests {

    @Test(expected = IllegalStateException.class)
    public void writeAfterClosedThrowsException() {
        PravegaWriter writer = prepareDummy();
        writer.close();
        writer.writeEvent("whatever");
    }

    @Test
    public void nullArgumentsCauseException() {

        assertThrows("NullPointerException did not occur.",
                () -> new PravegaWriter<String>(null, "test-stream", "dummy-controller-uri",
                        new JavaSerializer<String>()),
                e -> e instanceof NullPointerException);

        assertThrows("NullPointerException did not occur.",
                () -> new PravegaWriter<String>(null, "test-stream", "dummy-controller-uri",
                        new JavaSerializer<String>()),
                e -> e instanceof NullPointerException);

        assertThrows("NullPointerException did not occur.",
                () -> new PravegaWriter<String>("test-scope", null, "dummy-controller-uri",
                        new JavaSerializer<String>()),
                e -> e instanceof NullPointerException);

        assertThrows("NullPointerException did not occur.",
                () -> new PravegaWriter<String>("test-scope", "test-stream", null,
                        new JavaSerializer<String>()),
                e -> e instanceof NullPointerException);

        assertThrows("NullPointerException did not occur.",
                () -> new PravegaWriter<String>("test-scope", "test-stream",
                        "dummy-controller-uri", null),
                e -> e instanceof NullPointerException);
    }

    private PravegaWriter prepareDummy() {
        return new PravegaWriter("test-scope", "test-stream",
                "dummy-controller-uri", new JavaSerializer<String>());
    }
}
