package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.message.GrantOwnerPrivilegesResponseData;
import org.apache.kafka.common.protocol.*;

public class GrantOwnerPrivilegesResponse extends AbstractResponse {

    private final GrantOwnerPrivilegesResponseData data;

    public GrantOwnerPrivilegesResponse(GrantOwnerPrivilegesResponseData data) {
        super(ApiKeys.forId(data.apiKey()));
        this.data = data;
    }

    public GrantOwnerPrivilegesResponseData data() {
        return data;
    }

    public static GrantOwnerPrivilegesResponse parse(ByteBuffer buffer, short version) {
        return new GrantOwnerPrivilegesResponse(
                new GrantOwnerPrivilegesResponseData(new ByteBufferAccessor(buffer), version)
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
