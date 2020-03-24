/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.adapters.kafka.client.common;

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import lombok.NonNull;

public class ChecksumMaker {

    public static long computeCRC32Checksum(@NonNull String message) {
        return computeCRC32Checksum(message.getBytes());
    }

    public static long computeCRC32Checksum(@NonNull byte[] message) {
        Checksum checksum = new CRC32();
        checksum.update(message, 0, message.length);
        return checksum.getValue();
    }
}
