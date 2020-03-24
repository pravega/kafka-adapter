/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.samplesapps.shared;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class Utils {

    public static Properties loadConfigFromClasspath(String propertiesFileName) {
        Properties props = new Properties();
        try (InputStream input = Utils.class.getClassLoader()
                .getResourceAsStream(propertiesFileName)) {
            if (input == null) {
                log.error("Unable to find {} in classpath", propertiesFileName);
            }
            props.load(input);
        } catch (IOException e) {
            log.error("Unable to load {} from classpath", propertiesFileName);
            throw new RuntimeException(e);
        }
        return props;
    }
}
