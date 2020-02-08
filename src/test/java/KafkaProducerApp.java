import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.Properties;

@Slf4j
public class KafkaProducerApp {

    @Test
    public void publishAProducerRecord() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.224.222:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer(props);
        ProducerRecord message = new ProducerRecord("testtopic","My Message 1 - sent from program");
        producer.send(message);
        log.info("Done sending message...");

    }



}
