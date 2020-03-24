<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Kafka Adapter for Pravega

Preexisting Kafka producer/consumer applications can be made to write to and read from Pravega instead of Kafka, using this adapter. T do so, one has to make these changes: 

* Modify the dependency from Kafka Client library, to this adapter, and rebuild the application. 
* Modify the configuration to point to Pravega. This is typically done at deployment time. 
* Rerun/redeploy the application. 

As such, typically existing applications can be made to point to Pravega (instead of Kafka) without any application source-code level changes, especially if application configuration is externalized. 

This is how it works at a very high-level: 

* This adapter replaces Kafka implementations such as `org.apache.kafka.clients.producer.KafkaProducer` and `org.apache.kafka.clients.consumer.KafkaConsumer` with Pravega-specific implementations. 
* The adapter library packages Kafka client libraries and relocates Kafka and custom implementations using the Gradle shadow plugin. 

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
