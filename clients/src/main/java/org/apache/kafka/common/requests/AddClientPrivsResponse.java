package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.message.AddClientPrivsResponseData;
import org.apache.kafka.common.protocol.*;

public class AddClientPrivsResponse extends AbstractResponse {

    private final AddClientPrivsResponseData data;

    public AddClientPrivsResponse(AddClientPrivsResponseData data) {
        super(ApiKeys.forId(data.apiKey()));
        this.data = data;
    }

    public AddClientPrivsResponseData data() {
        return data;
    }

    public static AddClientPrivsResponse parse(ByteBuffer buffer, short version) {
        return new AddClientPrivsResponse(
                new AddClientPrivsResponseData(new ByteBufferAccessor(buffer), version)
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
