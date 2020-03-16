package io.pravega.adapters.kafka.client.shared;

import io.pravega.client.stream.Serializer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

@Slf4j
public class PravegaConsumerConfig extends PravegaKafkaConfig {

    @Getter
    private final Serializer serializer;

    public PravegaConsumerConfig(final Properties props) {
        super(props);
        serializer = this.instantiateSerde(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    }
}
