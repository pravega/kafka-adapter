<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Kafka Adapter

  * [Overview](#overview)
  * [How it Works](#how-it-works)
  * [Usage](#usage)
  * [Developing](#developing)
    + [Prerequisites](#prerequisites)
    + [Building Kafka Adapter](#building-kafka-adapter)
    + [Performing Checks](#performing-checks)

## Overview

Preexisting Kafka producer/consumer applications can be made to write to and read from Pravega instead of Kafka, using this adapter. To do so, one has to do these things: 

1. Modify the dependency from Kafka Client library, to this adapter, and rebuild the application. 
2. Modify the configuration to point to Pravega. This is typically done at deployment time. 
3. Rerun/redeploy the application. 

As such, Kafka applications that have application configuration externalized, can be made to point to Pravega (instead of Kafka) without any application source-code level changes. 

## How it Works

This is how it works at a very high-level: 

1. This adapter replaces Kafka implementations such as `org.apache.kafka.clients.producer.KafkaProducer` and `org.apache.kafka.clients.consumer.KafkaConsumer` with Pravega-specific implementations. 
2. The adapter library packages Kafka client libraries and relocates Kafka and custom implementations using the Gradle shadow plugin.
3. Assuming that the application has changed its dependency from Kafka Client libraries to the library containing this adapter, when the applicaton invokes Kafka producer/consumer methods (`org.apache.kafka.clients.producer.KafkaProducer` and `org.apache.kafka.clients.consumer.KafkaConsumer`), the custom implementations get invoked instead of the original Kafka implementations. These stand-in custom implementations then map Kafka calls to what makes sense for Pravega. 

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

Modify your Kafka Properties to target Pravega. Optionally, add Pravega-specific properties. More on this in documentation at a later time. The examples in `./kafka-adapter-samples` demonstrates its usage. 

## Developing 

### Prerequisites

* Java 8+

### Building Kafka Adapter

```bash
# Building the project
$ ./gradlew build

# Publishing the shadow jar with custom producer and consumer implementations to local maven repo. 
$ ./gradlew :kafka-adapter:publishShadowPublicationToMavenLocal
```

### Performing Checks

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
```
