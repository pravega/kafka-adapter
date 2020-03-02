package io.pravega.adapters.kafka.client.shared;

import java.util.Properties;

public class PravegaKafkaConfig {

    static String SCOPE = "pravega.scope";

    static String CONTROLLER_URI = "pravega.controller.uri";

    public static String DEFAULT_SCOPE = "migrated-from-kafka";

    public static String extractEndpoints(Properties props, String defaultValue) {
        String result = props.getProperty(PravegaProducerConfig.CONTROLLER_URI);
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
        return props.getProperty(PravegaProducerConfig.SCOPE, defaultValue);
    }
}
