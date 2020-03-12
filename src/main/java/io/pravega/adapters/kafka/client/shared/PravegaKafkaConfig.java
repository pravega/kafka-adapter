package io.pravega.adapters.kafka.client.shared;

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.JavaSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final Properties properties;

    public void setProperty(@NonNull String key, @NonNull String value) {
        this.properties.setProperty(key, value);
    }

    public String serverEndpoints() {
        return serverEndpoints(null);
    }

    public String serverEndpoints(String defaultValue) {
        String result = properties.getProperty(PravegaKafkaConfig.CONTROLLER_URI);
        if (result == null) {
            result = properties.getProperty("bootstrap.servers");
        }
        if (result == null && defaultValue != null) {
            result = defaultValue;
        }
        return result;
    }

    public String scope(String defaultValue) {
        return properties.getProperty(PravegaKafkaConfig.SCOPE, defaultValue);
    }

    @VisibleForTesting
    Serializer loadSerde(String key) {
        String serde = properties.getProperty(key);
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

    @SuppressFBWarnings(value = "IP_PARAMETER_IS_DEAD_BUT_OVERWRITTEN", justification = "No choice here")
    public <K, V> void populateProducerInterceptors(ProducerInterceptors<K, V> interceptors) {
        List<ProducerInterceptor<K, V>> ic = new ArrayList<ProducerInterceptor<K, V>>();

        // TODO: Support multiple producer interceptors?
        String producerInterceptorClass = properties.getProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
        if (producerInterceptorClass != null) {
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
    }

    public String groupId(String defaultValue) {
        return properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG, defaultValue);
    }

    public String clientId(String defaultValue) {
        return properties.getProperty(CommonClientConfigs.GROUP_ID_CONFIG, defaultValue);
    }
}
