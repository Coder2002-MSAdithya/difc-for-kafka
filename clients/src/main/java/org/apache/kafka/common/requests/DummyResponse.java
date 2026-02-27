package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.message.DummyResponseData;
import org.apache.kafka.common.protocol.*;

public class DummyResponse extends AbstractResponse {

    private final DummyResponseData data;

    public DummyResponse(DummyResponseData data) {
        super(ApiKeys.DUMMY);
        this.data = data;
    }

    public DummyResponseData data() {
        return data;
    }

    public static DummyResponse parse(ByteBuffer buffer, short version) {
        return new DummyResponse(
                new DummyResponseData(new ByteBufferAccessor(buffer), version)
        );
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return Collections.emptyMap();
    }

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.DUMMY;
    }

    @Override
    public int throttleTimeMs() {
        return 0;
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        // no-op
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
