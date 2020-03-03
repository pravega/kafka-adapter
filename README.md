# Kafka Adapter for Pravega

This code is from a proof-of-concept exercise for building a application adapter for migrating existing kafka producer and consumer applications without any material source-code changes. 

This adapter replaces `org.apache.kafka.clients.producer.KafkaProducer` and `org.apache.kafka.clients.consumer.KafkaConsumer` implementations with custom implementations that talk to Pravega instead of Kafka. 

## How to Build

```
# Building the project
$ ./gradlew build

# Publishing the shadow jar with custom producer and consumer implementations to local maven reo. 
$ ./gradlew publishShadowPublicationToMavenLocal
```

## Usage

Add the following to your dependencies list. Replace the version if necessary. 

```
compile group: "io.pravega.adapters.kafka", name: "kafka-adapter", version: "1.0-SNAPSHOT", classifier: "all"
```
