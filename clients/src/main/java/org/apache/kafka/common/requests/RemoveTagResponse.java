package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.message.RemoveTagResponseData;
import org.apache.kafka.common.protocol.*;

public class RemoveTagResponse extends AbstractResponse {

    private final RemoveTagResponseData data;

    public RemoveTagResponse(RemoveTagResponseData data) {
        super(ApiKeys.forId(data.apiKey()));
        this.data = data;
    }

    public RemoveTagResponseData data() {
        return data;
    }

    public static RemoveTagResponse parse(ByteBuffer buffer, short version) {
        return new RemoveTagResponse(
                new RemoveTagResponseData(new ByteBufferAccessor(buffer), version)
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
