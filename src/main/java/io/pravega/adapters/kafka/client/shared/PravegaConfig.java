package io.pravega.adapters.kafka.client.shared;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

@Slf4j
@Builder
public class PravegaConfig {

    public static final String CONTROLLER_URI = "pravega.controller.uri";

    public static final String SCOPE = "pravega.scope";

    public static final String DEFAULT_SCOPE = "migrated-from-kafka";

    public static final String NUM_SEGMENTS = "pravega.segments.count";

    @Getter
    private final String scope;

    @Getter
    private final String controllerUri;

    @Getter
    private final int numSegments;

    public static PravegaConfig getInstance(@NonNull Properties properties) {
        PravegaConfigBuilder builder = PravegaConfig.builder();

        if (properties.getProperty(CONTROLLER_URI) != null) {
            builder.controllerUri = properties.getProperty(CONTROLLER_URI);
        }

        if (properties.getProperty(SCOPE) != null) {
            builder.scope = properties.getProperty(SCOPE);
        }
        builder.numSegments = numSegments(properties);
        return builder.build();
    }

    private static int numSegments(Properties properties) {
        String value = properties.getProperty(NUM_SEGMENTS);

        if (value == null) {
            return -1;
        }

        if (value.matches("\\d+")) {
            return Integer.parseInt(value);
        } else {
            return -1;
        }
    }


}
