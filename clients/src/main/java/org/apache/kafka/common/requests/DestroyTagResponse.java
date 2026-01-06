package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.message.DestroyTagResponseData;
import org.apache.kafka.common.protocol.*;

public class DestroyTagResponse extends AbstractResponse {

    private final DestroyTagResponseData data;

    public DestroyTagResponse(DestroyTagResponseData data) {
        super(ApiKeys.forId(data.apiKey()));   // important: call AbstractResponse(ApiKeys)
        this.data = data;
    }

    public DestroyTagResponseData data() {
        return data;
    }

    public static DestroyTagResponse parse(ByteBuffer buffer, short version) {
        return new DestroyTagResponse(
                new DestroyTagResponseData(new ByteBufferAccessor(buffer), version)
        );
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Errors error = Errors.forCode(data.errorCode());
        if (error == Errors.NONE) {
            return Collections.emptyMap();
        }
        return Collections.singletonMap(error, 1);
    }

    @Override
    public int throttleTimeMs() {
        // No throttle field in your schema
        return 0;
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        // No-op because there is no throttle field to set
    }

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.forId(data.apiKey());
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
