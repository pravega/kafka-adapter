package io.pravega.adapters.kafka.client.shared;

import io.pravega.client.stream.impl.JavaSerializer;
import org.junit.Test;

import static org.junit.Assert.fail;

public class PravegaWriterTests {

    @Test(expected = IllegalStateException.class)
    public void writeAfterClosedThrowsException() {
        PravegaWriter writer = prepareDummy();
        writer.close();
        writer.writeEvent("whatever");
    }

    @Test
    public void nullArgumentsCauseException() {
        try {
            new PravegaWriter<String>(null, "test-stream", "dummy-controller-uri",
                    new JavaSerializer<String>());
            fail();
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            new PravegaWriter<String>("test-scope", null, "dummy-controller-uri",
                    new JavaSerializer<String>());
            fail();
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            new PravegaWriter<String>("test-scope", "test-stream", null,
                    new JavaSerializer<String>());
            fail();
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            new PravegaWriter<String>("test-scope", "test-stream", "dummy-controller-uri", null);
            fail();
        } catch (NullPointerException e) {
            // Expected
        }
    }

    private PravegaWriter prepareDummy() {
        return new PravegaWriter("test-scope", "test-stream",
                "dummy-controller-uri", new JavaSerializer<String>());
    }
}
