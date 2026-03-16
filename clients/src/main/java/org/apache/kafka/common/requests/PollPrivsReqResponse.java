package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.PollPrivsReqResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class PollPrivsReqResponse extends AbstractResponse {

    private final PollPrivsReqResponseData data;

    public PollPrivsReqResponse(PollPrivsReqResponseData data) {
        super(ApiKeys.POLL_PRIVS_REQ);
        this.data = data;
    }

    @Override
    public PollPrivsReqResponseData data() {
        return data;
    }

    public static PollPrivsReqResponse parse(ByteBuffer buffer, short version) {
        return new PollPrivsReqResponse(new PollPrivsReqResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return Collections.emptyMap();
    }

    @Override
    public int throttleTimeMs() {
        return 0;
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        // no-op
    }
}
