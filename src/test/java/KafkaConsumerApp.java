import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

@Slf4j
public class KafkaConsumerApp {

    @Test
    public void consumeAPreexistingMessage() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.224.222:9092");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "your_client_id");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer myConsumer = new KafkaConsumer(props);
        myConsumer.subscribe(Arrays.asList("testtopic"));

        // Alternatively, use regular expressions:
        // myConsumer.subscribe(“my-*”);

        try {
            while (true) {
                ConsumerRecords<String, String> records = myConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    log.info("******* " + record.value());
                }
                // Your processing logic goes here...
            }
        }
        finally {
            myConsumer.close();
        }
    }


}
