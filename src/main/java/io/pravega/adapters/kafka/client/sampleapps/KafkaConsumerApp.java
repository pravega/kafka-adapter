package io.pravega.adapters.kafka.client.sampleapps;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

@Slf4j
public class KafkaConsumerApp {

    public static void main (String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", System.getProperty("bootstrap.servers", "localhost:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "your_client_id");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer myConsumer = new KafkaConsumer(props);
        myConsumer.subscribe(Arrays.asList("testtopic"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = myConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Consumed a record containing value: {}", record.value());
                }
                // Business logic
            }
        }
        finally {
            myConsumer.close();
        }
    }


}
