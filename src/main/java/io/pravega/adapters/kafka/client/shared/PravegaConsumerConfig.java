package io.pravega.adapters.kafka.client.shared;

import io.pravega.client.stream.Serializer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

@Slf4j
public class PravegaConsumerConfig extends PravegaKafkaConfig {

    public static final String VALUE_DESERIALIZER =
            org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

    @Getter
    private final Serializer serializer;

    public PravegaConsumerConfig(final Properties props) {
        super(props);
        String valueSerializerFqcn = props.getProperty(VALUE_DESERIALIZER);
        if (valueSerializerFqcn == null || valueSerializerFqcn.trim().equals("")) {
            throw new IllegalArgumentException("Value serializer not specified");
        } else {
            serializer = this.instantiateSerde(props.getProperty(VALUE_DESERIALIZER));
        }
    }
}
