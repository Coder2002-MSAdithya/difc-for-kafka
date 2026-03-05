package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.message.GrantCapResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

public class GrantCapResponse extends AbstractResponse {

    private final GrantCapResponseData data;

    public GrantCapResponse(GrantCapResponseData data) {
        super(ApiKeys.forId(data.apiKey()));
        this.data = data;
    }

    public GrantCapResponseData data() {
        return data;
    }

    public static GrantCapResponse parse(ByteBuffer buffer, short version) {
        return new GrantCapResponse(
                new GrantCapResponseData(new ByteBufferAccessor(buffer), version)
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
