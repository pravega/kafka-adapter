package io.pravega.adapters.kafka.client.sampleapps;

import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class KafkaProducerApp {

    public static void main (String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", System.getProperty("bootstrap.servers", "localhost:9092"));
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer(props);
        ProducerRecord message = new ProducerRecord("testtopic", "My Message 1 - sent from program");
        log.info("Sending a producer record message");
        producer.send(message);
        log.info("Done sending message...");
        producer.close();
    }



}
