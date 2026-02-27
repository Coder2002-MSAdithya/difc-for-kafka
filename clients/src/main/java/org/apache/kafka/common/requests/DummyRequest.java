package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.message.DummyRequestData;
import org.apache.kafka.common.message.DummyResponseData;
import org.apache.kafka.common.protocol.*;

public class DummyRequest extends AbstractRequest {

    private final DummyRequestData data;

    public static class Builder extends AbstractRequest.Builder<DummyRequest> {

        private final DummyRequestData data;

        public Builder(DummyRequestData data) {
            super(ApiKeys.DUMMY);   // we will define this
            this.data = data;
        }

        @Override
        public DummyRequest build(short version) {
            return new DummyRequest(data, version);
        }

        public DummyRequestData data() {
            return data;
        }
    }

    public DummyRequest(DummyRequestData data, short version) {
        super(ApiKeys.DUMMY, version);
        this.data = data;
    }

    @Override
    public DummyRequestData data() {
        return data;
    }

    public static DummyRequest parse(ByteBuffer buffer, short version) {
        return new DummyRequest(
                new DummyRequestData(new ByteBufferAccessor(buffer), version),
                version
        );
    }

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.DUMMY;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new DummyResponse(
                new DummyResponseData()
                        .setMessage("Dummy request failed: " + e.getMessage())
        );
    }
}
