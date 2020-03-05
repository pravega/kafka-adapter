package io.pravega.adapters.kafka.client.shared;

import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.JavaSerializer;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Pravega-specific constants for adapter apps.
 */
@Slf4j
@RequiredArgsConstructor
public class PravegaKafkaConfig {

    public static final String VALUE_SERIALIZER = "value.serializer";

    public static final String SCOPE = "pravega.scope";

    public static final String CONTROLLER_URI = "pravega.controller.uri";

    public static final String DEFAULT_SCOPE = "migrated-from-kafka";

    public final Properties props;

    public String serverEndpoints() {
        return serverEndpoints(null);
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

    public Serializer serializer() {
        String serializerName = props.getProperty(VALUE_SERIALIZER);
        if (serializerName != null) {
            if (serializerName.equals("org.apache.kafka.common.serialization.StringSerializer")) {
                return new JavaSerializer<String>();
            } else {
                try {
                    return (Serializer) Class.forName(serializerName).newInstance();
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                    log.error("Unable to instantiate serializer with name [{}]", serializerName, e);
                    throw new IllegalStateException("e");
                }
            }
        } else {
            // The default serializer
            return new JavaSerializer<String>();
        }
    }


    /*public static <T> T instantiate(String name, Class<T> cls) throws ClassNotFoundException, IllegalAccessException,
            InstantiationException {
        return (T) Class.forName(name).newInstance();
    }

    public static Serializer<T> extractSerializer(Properties props, Class<T> cls) {
        String serializerName = props.getProperty(VALUE_SERIALIZER);
        if (serializerName == null) {
            // The default serializer
            return new JavaSerializer<String>();
        }

        if (serializerName.equals("org.apache.kafka.common.serialization.StringSerializer")) {
            return new JavaSerializer<String>();
        } else {
            try {
                // return (Serializer)Class.forName(serializerName).newInstance();
                return instantiate(serializerName, T);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new IllegalStateException("Unable to instantiate serilizer with name [" +
                        serializerName + "]");
            }
        }
    }*/
}
