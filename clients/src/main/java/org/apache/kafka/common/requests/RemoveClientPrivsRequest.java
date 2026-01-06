package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.message.RemoveClientPrivsRequestData;
import org.apache.kafka.common.message.RemoveClientPrivsResponseData;
import org.apache.kafka.common.protocol.*;

public class RemoveClientPrivsRequest extends AbstractRequest {

    private final RemoveClientPrivsRequestData data;

    public static class Builder extends AbstractRequest.Builder<RemoveClientPrivsRequest> {
        private final RemoveClientPrivsRequestData data;

        public Builder(RemoveClientPrivsRequestData data) {
            super(ApiKeys.forId(data.apiKey()));
            this.data = data;
        }

        @Override
        public RemoveClientPrivsRequest build(short version) {
            return new RemoveClientPrivsRequest(data, version);
        }
    }

    public RemoveClientPrivsRequest(RemoveClientPrivsRequestData data, short version) {
        super(ApiKeys.forId(data.apiKey()), version);
        this.data = data;
    }

    @Override
    public RemoveClientPrivsRequestData data() {
        return data;
    }

    public static RemoveClientPrivsRequest parse(ByteBuffer buffer, short version) {
        return new RemoveClientPrivsRequest(
                new RemoveClientPrivsRequestData(new ByteBufferAccessor(buffer), version),
                version
        );
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new RemoveClientPrivsResponse(
                new RemoveClientPrivsResponseData()
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message())
        );
    }
}

