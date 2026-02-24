package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.message.GetOwnListResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Writable;

public class GetOwnListResponse extends AbstractResponse {

    private final GetOwnListResponseData data;

    public GetOwnListResponse(GetOwnListResponseData data) {
        super(ApiKeys.forId(data.apiKey()));
        this.data = data;
    }

    public GetOwnListResponseData data() {
        return data;
    }

    public static GetOwnListResponse parse(ByteBuffer buffer, short version) {
        return new GetOwnListResponse(
                new GetOwnListResponseData(new ByteBufferAccessor(buffer), version)
        );
    }

    public void write(Writable writable, ObjectSerializationCache cache, short version) {
        data.write(writable, cache, version);
    }

    public int size(ObjectSerializationCache cache, short version) {
        return data.size(cache, version);
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
    public ApiKeys apiKey() {
        return ApiKeys.forId(data.apiKey());
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
