package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.PollPrivsReqRequestData;
import org.apache.kafka.common.message.PollPrivsReqResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;

public class PollPrivsReqRequest extends AbstractRequest {

    private final PollPrivsReqRequestData data;

    public static class Builder extends AbstractRequest.Builder<PollPrivsReqRequest> {
        private final PollPrivsReqRequestData data;

        public Builder(PollPrivsReqRequestData data) {
            super(ApiKeys.POLL_PRIVS_REQ);
            this.data = data;
        }

        @Override
        public PollPrivsReqRequest build(short version) {
            return new PollPrivsReqRequest(data, version);
        }

        public PollPrivsReqRequestData data() {
            return data;
        }
    }

    public PollPrivsReqRequest(PollPrivsReqRequestData data, short version) {
        super(ApiKeys.POLL_PRIVS_REQ, version);
        this.data = data;
    }

    @Override
    public PollPrivsReqRequestData data() {
        return data;
    }

    public static PollPrivsReqRequest parse(ByteBuffer buffer, short version) {
        return new PollPrivsReqRequest(new PollPrivsReqRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new PollPrivsReqResponse(new PollPrivsReqResponseData()
                .setTagName(null)
                .setCapability((byte) -1));
    }
}

