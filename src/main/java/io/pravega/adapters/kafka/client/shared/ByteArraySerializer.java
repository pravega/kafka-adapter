package io.pravega.adapters.kafka.client.shared;

import io.pravega.client.stream.Serializer;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class ByteArraySerializer implements Serializer<byte[]>, Serializable {
    private static final long serialVersionUID = 1L;

    public ByteArraySerializer() {}

    @Override
    public ByteBuffer serialize(byte[] value) {
        return ByteBuffer.wrap(value);
    }

    @Override
    public byte[] deserialize(ByteBuffer serializedValue) {
        byte[] result = new byte[serializedValue.remaining()];
        serializedValue.get(result);
        return result;
    }

    public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class clazz = Class.forName("io.pravega.adapters.kafka.client.shared.ByteArraySerializer");
        clazz.newInstance();
        // ((clazz.getConstructors()[0].getName()));
    }
}
