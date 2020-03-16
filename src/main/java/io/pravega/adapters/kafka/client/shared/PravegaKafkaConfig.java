package io.pravega.adapters.kafka.client.shared;

import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Properties;

@Slf4j
public abstract class PravegaKafkaConfig {

    @Getter
    protected final PravegaConfig pravegaConfig;

    @Getter(AccessLevel.PROTECTED)
    private final Properties properties;

    public PravegaKafkaConfig(Properties props) {
        if (props.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) == null) {
            throw new IllegalArgumentException(String.format("Property [%s] is not set",
                    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        }
        properties = props;
        pravegaConfig = PravegaConfig.getInstance(props);
    }

    public String getServerEndpoints() {
        if (pravegaConfig.getControllerUri() != null) {
            return pravegaConfig.getControllerUri();
        } else {
            return this.properties.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        }
    }

    public String getScope() {
        return pravegaConfig.getScope();
    }

    public String getGroupId(String defaultValue) {
        return properties.getProperty(CommonClientConfigs.GROUP_ID_CONFIG, defaultValue);
    }

    public String getClientId(String defaultValue) {
        return properties.getProperty(CommonClientConfigs.CLIENT_ID_CONFIG, defaultValue);
    }

    protected Serializer instantiateSerde(@NonNull String fqClassName) {
        if (fqClassName.equals("org.apache.kafka.common.serialization.StringSerializer") ||
                fqClassName.equals("org.apache.kafka.common.serialization.StringDeserializer")) {
            return new JavaSerializer<String>();
        } else {
            try {
                return (Serializer) Class.forName(fqClassName).newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                log.error("Unable to instantiate serializer with name [{}]", fqClassName, e);
                throw new IllegalStateException(e);
            }
        }
    }
}
