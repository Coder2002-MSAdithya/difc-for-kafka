package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.message.RegisterClientResponseData;
import org.apache.kafka.common.protocol.*;

public class RegisterClientResponse extends AbstractResponse {

    private final RegisterClientResponseData data;

    public RegisterClientResponse(RegisterClientResponseData data) {
        super(ApiKeys.forId(data.apiKey()));   // important: call AbstractResponse(ApiKeys)
        this.data = data;
    }

    public RegisterClientResponseData data() {
        return data;
    }

    public static RegisterClientResponse parse(ByteBuffer buffer, short version) {
        return new RegisterClientResponse(
                new RegisterClientResponseData(new ByteBufferAccessor(buffer), version)
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
