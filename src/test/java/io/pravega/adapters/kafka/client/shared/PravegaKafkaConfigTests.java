package io.pravega.adapters.kafka.client.shared;

import io.pravega.adapters.kafka.client.utils.PersonSerializer;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.JavaSerializer;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class PravegaKafkaConfigTests {

    @Test
    public void returnsStringSerializerByDefault() {
        PravegaKafkaConfig config = new PravegaKafkaConfig(new Properties());
        Serializer serializer = config.serializer();
        assertTrue(serializer instanceof JavaSerializer);
    }

    @Test
    public void loadsAndReturnsSpecifiedSerialize() {
        Properties props = new Properties();
        props.setProperty("value.serializer", "io.pravega.adapters.kafka.client.utils.PersonSerializer");
        PravegaKafkaConfig config = new PravegaKafkaConfig(props);
        Serializer serializer = config.serializer();
        assertTrue(serializer instanceof PersonSerializer);
    }
}
