# Kafka Adapter for Pravega

This code is from a proof-of-concept exercise for building a application adapter for migrating existing kafka producer and consumer applications without any material source-code changes. 

This adapter replaces `org.apache.kafka.clients.producer.KafkaProducer` and `org.apache.kafka.clients.consumer.KafkaConsumer` implementations with custom implementations that talk to Pravega instead of Kafka. 

