package io.pravega.adapters.kafka.client.shared;

import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.JavaSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;

/**
 * Pravega-specific constants for adapter apps.
 */
@Slf4j
@RequiredArgsConstructor
public class PravegaKafkaConfig {

    public static final String VALUE_SERIALIZER = "value.serializer";
    public static final String VALUE_DESERIALIZER = "value.deserializer";

    public static final String SCOPE = "pravega.scope";

    public static final String CONTROLLER_URI = "pravega.controller.uri";

    public static final String DEFAULT_SCOPE = "migrated-from-kafka";

    private final Properties props;

    public String serverEndpoints() {
        return serverEndpoints(null);
    }

    public void setProperty(String key, String value) {
        this.props.setProperty(key, value);
    }

    public String serverEndpoints(String defaultValue) {
        String result = props.getProperty(PravegaKafkaConfig.CONTROLLER_URI);
        if (result == null) {
            result = props.getProperty("bootstrap.servers");
        }
        if (result == null) {
            if (defaultValue == null || defaultValue.trim().equals("")) {
                throw new IllegalArgumentException("Properties does not contain server endpoint(s), " +
                        "and default value is null/empty");
            } else {
                result = defaultValue;
            }
        }
        return result;
    }

    public String scope(String defaultValue) {
        return props.getProperty(PravegaKafkaConfig.SCOPE, defaultValue);
    }

    private Serializer loadSerde(String key) {
        String serde = props.getProperty(key);
        if (serde != null) {
            if (serde.equals("org.apache.kafka.common.serialization.StringSerializer") ||
            serde.equals("org.apache.kafka.common.serialization.StringDeserializer")) {
                return new JavaSerializer<String>();
            } else {
                try {
                    return (Serializer) Class.forName(serde).newInstance();
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                    log.error("Unable to instantiate serializer with name [{}]", serde, e);
                    throw new IllegalStateException(e);
                }
            }
        } else {
            // The default serializer
            return new JavaSerializer<String>();
        }
    }

    public Serializer deserializer() {
        return loadSerde(VALUE_DESERIALIZER);
    }

    public Serializer serializer() {
        return loadSerde(VALUE_SERIALIZER);
    }

    public <K, V> void populateProducerInterceptors(ProducerInterceptors<K, V> interceptors) {
        List<ProducerInterceptor<K, V>> ic = new ArrayList<ProducerInterceptor<K, V>>();

        // TODO: Support multiple producer interceptors?
        String producerInterceptorClass = props.getProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
        try {
            ProducerInterceptor<K, V> interceptor =
                    (ProducerInterceptor) Class.forName(producerInterceptorClass).newInstance();
            ic.add(interceptor);
            log.debug("Adding interceptor [{}] to the producer interceptor list", interceptor);
            interceptors = new ProducerInterceptors<K, V>(ic);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            log.error("Unable to instantiate producer interceptor with name [{}]", producerInterceptorClass, e);
            throw new IllegalStateException(e);
        }
    }

    public List<ConsumerInterceptor> consumerInterceptors() {
        // TODO: Implement
        return new ArrayList<>();
    }
}
