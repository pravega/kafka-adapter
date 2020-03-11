package io.pravega.adapters.kafka.client.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UtilsTests {

    @Test(expected = NullPointerException.class)
    public void testComputeCrcChecksumThrowsExceptionWhenInputIsNull() {
        String value = null;
        Utils.computeCRC32Checksum(value);
    }

    @Test(expected = NullPointerException.class)
    public void testComputeCrcChecksumThrowsExceptionWhenInputBytesIsNull() {
        byte[] value = null;
        Utils.computeCRC32Checksum(value);
    }

    @Test
    public void testComputeCrcChecksumWorksCorrectly() {
        String value = "The quick brown fox jumps over the lazy dog";
        long crc32 = Utils.computeCRC32Checksum(value);
        assertEquals("414fa339", Long.toHexString(crc32));
    }
}
