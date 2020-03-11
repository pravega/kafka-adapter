package io.pravega.adapters.kafka.client.utils;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

@Slf4j
public class FakeKafkaProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        log.debug("onSend: record={}", record);
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        log.debug("onAcknowledgement: metadata={}, exception={}", metadata, exception);
    }

    @Override
    public void close() {
        log.debug("Closing");

    }

    @Override
    public void configure(Map<String, ?> configs) {
        log.debug("Configuring: configs={}", configs);
    }
}
