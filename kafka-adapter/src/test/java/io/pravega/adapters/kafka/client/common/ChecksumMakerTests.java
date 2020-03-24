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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ChecksumMakerTests {

    @Test(expected = NullPointerException.class)
    public void testComputeCrcChecksumThrowsExceptionWhenInputIsNull() {
        String value = null;
        ChecksumMaker.computeCRC32Checksum(value);
    }

    @Test(expected = NullPointerException.class)
    public void testComputeCrcChecksumThrowsExceptionWhenInputBytesIsNull() {
        byte[] value = null;
        ChecksumMaker.computeCRC32Checksum(value);
    }

    @Test
    public void testComputeCrcChecksumWorksCorrectly() {
        String value = "The quick brown fox jumps over the lazy dog";
        long crc32 = ChecksumMaker.computeCRC32Checksum(value);
        assertEquals("414fa339", Long.toHexString(crc32));
    }
}
