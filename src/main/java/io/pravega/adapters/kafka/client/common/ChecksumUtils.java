package io.pravega.adapters.kafka.client.common;

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import lombok.NonNull;

public class ChecksumUtils {

    public static long computeCRC32Checksum(@NonNull String message) {
        return computeCRC32Checksum(message.getBytes());
    }

    public static long computeCRC32Checksum(@NonNull byte[] message) {
        Checksum checksum = new CRC32();
        checksum.update(message, 0, message.length);
        return checksum.getValue();
    }
}
