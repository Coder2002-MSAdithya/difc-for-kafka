package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.message.RemoveTagRequestData;
import org.apache.kafka.common.message.RemoveTagResponseData;
import org.apache.kafka.common.protocol.*;

public class RemoveTagRequest extends AbstractRequest {

    private final RemoveTagRequestData data;

    public static class Builder extends AbstractRequest.Builder<RemoveTagRequest> {
        private final RemoveTagRequestData data;

        public Builder(RemoveTagRequestData data) {
            super(ApiKeys.forId(data.apiKey()));
            this.data = data;
        }

        @Override
        public RemoveTagRequest build(short version) {
            return new RemoveTagRequest(data, version);
        }
    }

    public RemoveTagRequest(RemoveTagRequestData data, short version) {
        super(ApiKeys.forId(data.apiKey()), version);
        this.data = data;
    }

    @Override
    public RemoveTagRequestData data() {
        return data;
    }

    public static RemoveTagRequest parse(ByteBuffer buffer, short version) {
        return new RemoveTagRequest(
                new RemoveTagRequestData(new ByteBufferAccessor(buffer), version),
                version
        );
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new RemoveTagResponse(
                new RemoveTagResponseData()
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message())
        );
    }
}

