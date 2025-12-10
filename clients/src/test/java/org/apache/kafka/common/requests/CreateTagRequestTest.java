/*
 * Licensed to the Apache Software Foundation (ASF) ...
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.CreateTagRequestData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CreateTagRequestTest {

    private static Stream<Arguments> CreateTagRequestVersions() {
        return IntStream
                .range(CreateTagRequestData.LOWEST_SUPPORTED_VERSION,
                        CreateTagRequestData.HIGHEST_SUPPORTED_VERSION + 1)
                .mapToObj(version -> Arguments.of((short) version));
    }

    @ParameterizedTest
    @MethodSource("CreateTagRequestVersions")
    public void testBasicBuild(short version) {
        CreateTagRequestData data = new CreateTagRequestData()
                .setTagName("test-tag");

        CreateTagRequestData data2 = readSerializedRequest(version, data);

        assertEquals("test-tag", data2.tagName(), "Unexpected tag name in " + data2);
    }

    static CreateTagRequestData readSerializedRequest(
            short version,
            CreateTagRequestData input
    ) {
        CreateTagRequest.Builder builder = new CreateTagRequest.Builder(input);

        // You can assert version bounds if you like; here it is trivial (only v0)
        assertEquals(CreateTagRequestData.LOWEST_SUPPORTED_VERSION, builder.oldestAllowedVersion());
        assertEquals(CreateTagRequestData.HIGHEST_SUPPORTED_VERSION, builder.latestAllowedVersion());

        CreateTagRequest request = builder.build(version);

        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = request.data().size(cache, version);
        ByteBuffer buf = ByteBuffer.allocate(size);
        ByteBufferAccessor accessor = new ByteBufferAccessor(buf);
        request.data().write(accessor, cache, version);

        CreateTagRequestData data2 = new CreateTagRequestData();
        buf.flip();
        data2.read(accessor, version);
        return data2;
    }
}
