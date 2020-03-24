/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.adapters.kafka.client.shared;

import io.pravega.adapters.kafka.client.utils.Person;
import io.pravega.adapters.kafka.client.utils.PersonSerializer;
import io.pravega.client.stream.Serializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.Test;

import java.util.Properties;

import static io.pravega.adapters.kafka.client.utils.TestUtils.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

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
        assertEquals("dummy_uri", config.getServerEndpoints());
        assertEquals("dummy_groupid", config.getGroupId(null));
        assertEquals("dummy_clientid", config.getClientId(null));
    }

    @Test
    public void instantiateSerdeFailsIfInputIsInvalid() {
        assertThrows("Didn't encounter expected exception.",
                () -> {
                    Properties props = new Properties();
                    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "dummy_uri");
                    PravegaKafkaConfig config = new FakeKafkaConfig(props);

                    config.instantiateSerde(null);
                },
                e -> e instanceof NullPointerException);

        assertThrows("Didn't encounter expected exception.",
                () -> {
                    Properties props = new Properties();
                    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "dummy_uri");
                    PravegaKafkaConfig config = new FakeKafkaConfig(props);

                    config.instantiateSerde("random");
                },
                e -> e instanceof IllegalArgumentException);
    }

    @Test
    public void instantiateSerdeFromClassNameSucceeds() {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "dummy_uri");
        props.setProperty("some-key", "io.pravega.adapters.kafka.client.utils.PersonSerializer");

        PravegaKafkaConfig config = new FakeKafkaConfig(props);
        Serializer<Person> serializer = config.instantiateSerde("some-key");

        assertNotNull(serializer);
        assertNotEquals(Person.class, serializer.getClass());
    }

    @Test
    public void instantiateSerdeFromClassSucceeds() {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "dummy_uri");
        props.put("some-key", PersonSerializer.class);

        PravegaKafkaConfig config = new FakeKafkaConfig(props);
        Serializer<Person> serializer = config.instantiateSerde("some-key");

        assertNotNull(serializer);
        assertNotEquals(Person.class, serializer.getClass());
    }

    @Test
    public void instantiateSerdeFromObjectSucceeds() {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "dummy_uri");
        props.put("some-key", new PersonSerializer());

        PravegaKafkaConfig config = new FakeKafkaConfig(props);
        Serializer<Person> serializer = config.instantiateSerde("some-key");

        assertNotNull(serializer);
        assertNotEquals(Person.class, serializer.getClass());
    }
}

class FakeKafkaConfig extends PravegaKafkaConfig {

    public FakeKafkaConfig(Properties props) {
        super(props);
    }
}
