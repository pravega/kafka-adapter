package io.pravega.adapters.kafka.client.shared;

import io.pravega.adapters.kafka.client.utils.PersonSerializer;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.JavaSerializer;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.junit.Test;

import static io.pravega.adapters.kafka.client.utils.TestUtils.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PravegaKafkaConfigTests {

    @Test
    public void loadSerdeReturnsJavaSerializerForStringSerde() {
        PravegaKafkaConfig config = new PravegaKafkaConfig(new Properties());
        assertTrue(config.loadSerde("io.pravega.adapters.kafka.client.utils.StringSerializer")
                instanceof JavaSerializer);
        assertTrue(config.loadSerde("io.pravega.adapters.kafka.client.utils.StringDeserializer")
                instanceof JavaSerializer);
    }

    @Test(expected = IllegalStateException.class)
    public void loadSerdeThrowsExceptionWhenSpecifiedSerializerIsNotFound() {
        PravegaKafkaConfig config = new PravegaKafkaConfig(new Properties());
        config.setProperty("serializer", "random.serializer");
        config.loadSerde("serializer");
    }

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

    @Test
    public void populatingProducerInterceptor() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.pravega.adapters.kafka.client.utils.FakeKafkaProducerInterceptor");
        PravegaKafkaConfig config = new PravegaKafkaConfig(props);
        ProducerInterceptors<String, String> interceptors = new ProducerInterceptors<String, String>(null);
        config.populateProducerInterceptors(interceptors);
        assertNotNull(interceptors);
    }

    @Test
    public void setPropertyUpdatesConfig() {
        PravegaKafkaConfig config = new PravegaKafkaConfig(new Properties());
        config.setProperty("key", "value");
        assertEquals("value", config.getProperties().getProperty("key"));
    }

    @Test
    public void setPropertyThrowsExceptionForNullInputs() {
        PravegaKafkaConfig config = new PravegaKafkaConfig(new Properties());
        assertThrows("Didn't encounter expected NullPointerException.",
                () -> config.setProperty(null, "value"),
                e -> e instanceof NullPointerException);

        assertThrows("Didn't encounter expected NullPointerException.",
                () -> config.setProperty("key", null),
                e -> e instanceof NullPointerException);
    }

    @Test
    public void serverEndpointsReturnsPravegaConfigValueFirst() {
        PravegaKafkaConfig config = new PravegaKafkaConfig(new Properties());
        config.setProperty(PravegaKafkaConfig.CONTROLLER_URI, "pravega://localhost:9090");
        config.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        assertEquals("pravega://localhost:9090", config.serverEndpoints());
    }

    @Test
    public void serverEndpointsReturnsKafkaConfigValueIfPravegaSpecificConfigUnspecified() {
        PravegaKafkaConfig config = new PravegaKafkaConfig(new Properties());
        config.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        assertEquals("localhost:9092", config.serverEndpoints());
    }

    @Test
    public void serverEndpointsReturnsSpecifiedDefaultValue() {
        PravegaKafkaConfig config = new PravegaKafkaConfig(new Properties());
        assertEquals("pravega://localhost:9090", config.serverEndpoints("pravega://localhost:9090"));
    }

    @Test
    public void numSegmentsReturnsNegativeValueIfConfigNotSpecified() {
        PravegaKafkaConfig config = new PravegaKafkaConfig(new Properties());
        assertEquals(-1, config.numSegments());
    }

    @Test
    public void numSegmentsReturnsNegativeValueIfConfigIsNotInteger() {
        PravegaKafkaConfig config = new PravegaKafkaConfig(new Properties());
        config.setProperty(PravegaKafkaConfig.NUM_SEGMENTS, "Str");
        assertEquals(-1, config.numSegments());

        config.setProperty(PravegaKafkaConfig.NUM_SEGMENTS, "1.2");
        assertEquals(-1, config.numSegments());
    }

    @Test
    public void numSegmentsReturnsValueIfConfigIsValid() {
        PravegaKafkaConfig config = new PravegaKafkaConfig(new Properties());
        config.setProperty(PravegaKafkaConfig.NUM_SEGMENTS, "0");
        assertEquals(0, config.numSegments());

        config.setProperty(PravegaKafkaConfig.NUM_SEGMENTS, "1");
        assertEquals(1, config.numSegments());
    }

}
