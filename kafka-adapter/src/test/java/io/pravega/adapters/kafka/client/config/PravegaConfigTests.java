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

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class PravegaConfigTests {

    @Test
    public void instantiatesDirectly() {
        PravegaConfig config = PravegaConfig.builder().build();
        assertNotNull(config);
    }

    @Test
    public void instantiatesWithEmptyProperties() {
        PravegaConfig config = PravegaConfig.getInstance(new Properties());
        assertNotNull(config);
    }

    @Test(expected = NullPointerException.class)
    public void instantiationWithNullPropertiesFails() {
        PravegaConfig config = PravegaConfig.getInstance(null);
    }

    @Test
    public void returnsConfiguredProperties() {
        Properties props = new Properties();
        props.setProperty(PravegaConfig.CONTROLLER_URI, "dummy_uri");
        props.setProperty(PravegaConfig.SCOPE, "dummy_scope");
        props.setProperty(PravegaConfig.NUM_SEGMENTS, "2");

        PravegaConfig config = PravegaConfig.getInstance(props);
        assertEquals("dummy_uri", config.getControllerUri());
        assertEquals("dummy_scope", config.getScope());
        assertEquals(2, config.getNumSegments());
    }

    @Test
    public void returnsDefaultProperties() {
        PravegaConfig config = PravegaConfig.getInstance(new Properties());
        assertNull(config.getControllerUri());
        assertNull(config.getScope());
        assertEquals(-1, config.getNumSegments());
    }

    @Test
    public void numSegmentsReturnsNegativeValueIfConfigNotSpecified() {
        PravegaConfig config = PravegaConfig.getInstance(new Properties());
        assertEquals(-1, config.getNumSegments());
    }

    @Test
    public void numSegmentsReturnsNegativeValueIfConfigIsNotInteger() {
        Properties props = new Properties();
        props.setProperty(PravegaConfig.NUM_SEGMENTS, "Str");
        PravegaConfig config = PravegaConfig.getInstance(props);
        assertEquals(-1, config.getNumSegments());

        props.setProperty(PravegaConfig.NUM_SEGMENTS, "1.2");
        assertEquals(-1, config.getNumSegments());
    }
}
