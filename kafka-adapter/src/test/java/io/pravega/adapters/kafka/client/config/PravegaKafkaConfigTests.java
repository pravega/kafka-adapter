/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.adapters.kafka.client.config;

import io.pravega.adapters.kafka.client.testutils.Person;
import io.pravega.adapters.kafka.client.testutils.PersonSerializer;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.client.stream.impl.JavaSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Properties;

import static io.pravega.adapters.kafka.client.testutils.TestUtils.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PravegaKafkaConfigTests {

    @Test(expected = IllegalArgumentException.class)
    public void noBootstrapServerConfigFailsConstruction() {
        PravegaKafkaConfig config = new FakeKafkaConfig(new Properties());
    }

    @Test
    public void specifiedPropertiesArePopulated() {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "dummy_uri");
        props.setProperty(CommonClientConfigs.GROUP_ID_CONFIG, "dummy_groupid");
        props.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "dummy_clientid");

        PravegaKafkaConfig config = new FakeKafkaConfig(props);
        assertEquals("dummy_uri", config.evaluateServerEndpoints());
        assertEquals("dummy_groupid", config.evaluateGroupId(null));
        assertEquals("dummy_clientid", config.evaluateClientId(null));
    }

    @Test
    public void instantiateSerdeFailsIfInputIsInvalid() {
        assertThrows("Didn't encounter expected exception.",
                () -> {
                    Properties props = new Properties();
                    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "dummy_uri");
                    PravegaKafkaConfig config = new FakeKafkaConfig(props);

                    config.evaluateSerde(null);
                },
                e -> e instanceof NullPointerException);

        assertThrows("Didn't encounter expected exception.",
                () -> {
                    Properties props = new Properties();
                    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "dummy_uri");
                    PravegaKafkaConfig config = new FakeKafkaConfig(props);

                    config.evaluateSerde("random");
                },
                e -> e instanceof IllegalArgumentException);
    }

    @Test
    public void instantiateSerdeFromPrimitiveSerdeNamesSucceeds() {
        verifySerdeCanLoadFromClassName("org.apache.kafka.common.serialization.IntegerSerializer",
                "org.apache.kafka.common.serialization.IntegerDeserializer");

        verifySerdeCanLoadFromClassName("org.apache.kafka.common.serialization.FloatSerializer",
                "org.apache.kafka.common.serialization.FloatDeserializer");

        verifySerdeCanLoadFromClassName("org.apache.kafka.common.serialization.LongSerializer",
                "org.apache.kafka.common.serialization.LongDeserializer");

        verifySerdeCanLoadFromClassName("org.apache.kafka.common.serialization.DoubleSerializer",
                "org.apache.kafka.common.serialization.DoubleDeserializer");

        verifySerdeCanLoadFromClassName("org.apache.kafka.common.serialization.ShortSerializer",
                "org.apache.kafka.common.serialization.ShortDeserializer");

        verifySerdeCanLoadFromClassName("org.apache.kafka.common.serialization.StringSerializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
    }

    @Test
    public void instantiateSerdeFromPrimitiveSerdeClassesSucceeds() {
        // Numeric ones
        verifySerdeCanLoadFromClass(IntegerSerializer.class, IntegerDeserializer.class);
        verifySerdeCanLoadFromClass(FloatSerializer.class, FloatDeserializer.class);
        verifySerdeCanLoadFromClass(LongSerializer.class, LongSerializer.class);
        verifySerdeCanLoadFromClass(DoubleSerializer.class, DoubleDeserializer.class);
        verifySerdeCanLoadFromClass(ShortSerializer.class, ShortDeserializer.class);

        verifySerdeCanLoadFromClass(StringSerializer.class, StringDeserializer.class);
    }

    @Test
    public void instantiateSerdeFromByteArraySerde() {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "dummy_uri");
        props.setProperty("serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.setProperty("deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        PravegaKafkaConfig config = new FakeKafkaConfig(props);
        Serializer<byte[]> serializer = config.evaluateSerde("serializer");
        Serializer<byte[]> deserializer = config.evaluateSerde("deserializer");

        assertNotNull(serializer);
        assertTrue(serializer instanceof ByteArraySerializer);
        assertNotNull(deserializer);
        assertTrue(deserializer instanceof ByteArraySerializer);

        props.put("serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class);
        props.put("deserializer", org.apache.kafka.common.serialization.ByteArrayDeserializer.class);

        config = new FakeKafkaConfig(props);
        serializer = config.evaluateSerde("serializer");
        deserializer = config.evaluateSerde("deserializer");

        assertNotNull(serializer);
        assertTrue(serializer instanceof ByteArraySerializer);
        assertNotNull(deserializer);
        assertTrue(deserializer instanceof ByteArraySerializer);
    }

    @Test
    public void instantiateSerdeFromByteBufferSerde() {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "dummy_uri");
        props.setProperty("serializer", "org.apache.kafka.common.serialization.ByteBufferSerializer");
        props.setProperty("deserializer", "org.apache.kafka.common.serialization.ByteBufferDeserializer");

        PravegaKafkaConfig config = new FakeKafkaConfig(props);
        Serializer<ByteBuffer> serializer = config.evaluateSerde("serializer");
        Serializer<ByteBuffer> deserializer = config.evaluateSerde("deserializer");

        assertNotNull(serializer);
        assertTrue(serializer instanceof ByteBufferSerializer);
        assertNotNull(deserializer);
        assertTrue(deserializer instanceof ByteBufferSerializer);

        props.put("serializer", org.apache.kafka.common.serialization.ByteBufferSerializer.class);
        props.put("deserializer", org.apache.kafka.common.serialization.ByteBufferDeserializer.class);

        config = new FakeKafkaConfig(props);
        serializer = config.evaluateSerde("serializer");
        deserializer = config.evaluateSerde("deserializer");

        assertNotNull(serializer);
        assertTrue(serializer instanceof ByteBufferSerializer);
        assertNotNull(deserializer);
        assertTrue(deserializer instanceof ByteBufferSerializer);
    }

    @Test
    public void instantiateSerdeFromCustomClassNameSucceeds() {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "dummy_uri");
        props.setProperty("some-key", "io.pravega.adapters.kafka.client.testutils.PersonSerializer");

        PravegaKafkaConfig config = new FakeKafkaConfig(props);
        Serializer<Person> serializer = config.evaluateSerde("some-key");

        assertNotNull(serializer);
        assertNotEquals(Person.class, serializer.getClass());
    }

    @Test
    public void instantiateSerdeFromCustomClassSucceeds() {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "dummy_uri");
        props.put("some-key", PersonSerializer.class);

        PravegaKafkaConfig config = new FakeKafkaConfig(props);
        Serializer<Person> serializer = config.evaluateSerde("some-key");

        assertNotNull(serializer);
        assertNotEquals(Person.class, serializer.getClass());
    }

    @Test
    public void instantiateSerdeFromObjectSucceeds() {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "dummy_uri");
        props.put("some-key", new PersonSerializer());

        PravegaKafkaConfig config = new FakeKafkaConfig(props);
        Serializer<Person> serializer = config.evaluateSerde("some-key");

        assertNotNull(serializer);
        assertNotEquals(Person.class, serializer.getClass());
    }

    private void verifySerdeCanLoadFromClassName(String serializerClassName, String deserializerClassName) {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "dummy_uri");
        props.setProperty("serializer", serializerClassName);
        props.setProperty("deserializer", deserializerClassName);
        verifySerde(props);
    }

    private void verifySerdeCanLoadFromClass(Class serializerClass, Class deserializerClass) {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "dummy_uri");
        props.put("serializer", serializerClass);
        props.put("deserializer", deserializerClass);
        verifySerde(props);
    }

    private void verifySerde(Properties props) {
        PravegaKafkaConfig config = new FakeKafkaConfig(props);
        Serializer<Integer> serializer = config.evaluateSerde("serializer");
        Serializer<Integer> deserializer = config.evaluateSerde("deserializer");

        assertNotNull(serializer);
        assertTrue(serializer instanceof JavaSerializer);

        assertNotNull(deserializer);
        assertTrue(deserializer instanceof JavaSerializer);
    }
}

class FakeKafkaConfig extends PravegaKafkaConfig {

    public FakeKafkaConfig(Properties props) {
        super(props);
    }
}
