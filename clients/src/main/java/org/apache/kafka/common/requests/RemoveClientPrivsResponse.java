package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.message.RemoveClientPrivsResponseData;
import org.apache.kafka.common.protocol.*;

public class RemoveClientPrivsResponse extends AbstractResponse {

    private final RemoveClientPrivsResponseData data;

    public RemoveClientPrivsResponse(RemoveClientPrivsResponseData data) {
        super(ApiKeys.forId(data.apiKey()));
        this.data = data;
    }

    public RemoveClientPrivsResponseData data() {
        return data;
    }

    public static RemoveClientPrivsResponse parse(ByteBuffer buffer, short version) {
        return new RemoveClientPrivsResponse(
                new RemoveClientPrivsResponseData(new ByteBufferAccessor(buffer), version)
        );
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Errors error = Errors.forCode(data.errorCode());
        if (error == Errors.NONE)
            return Collections.emptyMap();
        return Collections.singletonMap(error, 1);
    }

    @Override
    public int throttleTimeMs() {
        return 0;
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) { }

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.forId(data.apiKey());
    }
}
