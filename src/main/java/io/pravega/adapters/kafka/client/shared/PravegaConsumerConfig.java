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

    @Getter
    private final int readTimeoutInMs;

    @Getter
    private final int maxPollRecords;

    public PravegaConsumerConfig(final Properties props) {
        super(props);
        serializer = this.instantiateSerde(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        readTimeoutInMs = this.getPravegaConfig().getReadTimeoutInMs();
        maxPollRecords = extractMaxPollRecords(props);
    }

    private static int extractMaxPollRecords(Properties properties) {
        String value = properties.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
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
