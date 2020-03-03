# Kafka Adapter for Pravega

This code is from a proof-of-concept exercise for building an application adapter for migrating existing kafka producer and consumer applications without any material source-code changes. 

This adapter replaces `org.apache.kafka.clients.producer.KafkaProducer` and `org.apache.kafka.clients.consumer.KafkaConsumer` implementations with custom implementations that talk to Pravega instead of Kafka. Kafka producer and consumer applications can be made to talk to Pravega (instead of Kafka), merely by replacing Kafka dependencies in their builds with this adapter, modifying client application configuration, and rebuilding their application.

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
