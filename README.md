<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Kafka Adapter for Pravega

This code is from a proof-of-concept exercise for building an application adapter for migrating existing kafka producer and consumer applications without any material source-code changes. 

This adapter replaces `org.apache.kafka.clients.producer.KafkaProducer` and `org.apache.kafka.clients.consumer.KafkaConsumer` implementations with custom implementations that talk to Pravega instead of Kafka. Kafka producer and consumer applications can be made to talk to Pravega (instead of Kafka), merely by replacing Kafka dependencies in their builds with this adapter, modifying client application configuration, and rebuilding their application.

## How to Build

```bash
# Building the project
$ ./gradlew build

# Publishing the shadow jar with custom producer and consumer implementations to local maven repo. 
$ ./gradlew :kafka-adapter:publishShadowPublicationToMavenLocal
```

## Usage

Add the following to your dependencies list. Replace the version if necessary. 

Gradle: 
```groovy
compile group: "io.pravega.adapters.kafka-adapter", name: "kafka-adapter", version: "1.0-SNAPSHOT", classifier: "all"
```

Maven: 
```xml
<dependency>
    <groupId>io.pravega.adapters.kafka-adapter</groupId>
    <artifactId>kafka-adapter</artifactId>
    <version>1.0-SNAPSHOT</version>
    <classifier>all</classifier>
</dependency>
```

# Code Analysis and Test Coverage

```bash
# Running Checkstyle for the adapter module
$ ./gradlew :kafka-adapter:checkstyleMain
$ ./gradlew :kafka-adapter:checkstyleTest

# Running Spotbugs for the adapter module
$ ./gradlew :kafka-adapter:spotbugsMain
$ ./gradlew :kafka-adapter:spotbugsTest

# Running all checks
$ ./gradlew check

# Generating unit test coverage report. Output can be found in build/jococoHtml
$ ./gradlew jacocoTestReport

# Seeing other available tasks
$ ./gradlew tasks
```
