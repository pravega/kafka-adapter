package io.pravega.adapters.kafka.client.shared;

import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.JavaSerializer;

import java.util.Properties;

/**
 * Pravega-specific constants for adapter apps.
 */
public class PravegaKafkaConfig {

    public static final String VALUE_SERIALIZER = "value.serializer";

    public static final String SCOPE = "pravega.scope";

    public static final String CONTROLLER_URI = "pravega.controller.uri";

    public static final String DEFAULT_SCOPE = "migrated-from-kafka";

    public static String extractEndpoints(Properties props, String defaultValue) {
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

    public static String extractScope(Properties props, String defaultValue) {
        return props.getProperty(PravegaKafkaConfig.SCOPE, defaultValue);
    }

    public static Serializer extractSerializer(Properties props) {
        String serializerName = props.getProperty(VALUE_SERIALIZER);
        if (serializerName == null) {
            return new JavaSerializer<String>();
        }

        if (serializerName.equals("org.apache.kafka.common.serialization.StringSerializer")) {
            return new JavaSerializer<String>();
        } else {
            // TODO: Always returns the same serializer
            return new JavaSerializer<String>();
        }
    }
}
