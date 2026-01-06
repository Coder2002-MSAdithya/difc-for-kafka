package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.message.AddTagRequestData;
import org.apache.kafka.common.message.AddTagResponseData;
import org.apache.kafka.common.protocol.*;

public class AddTagRequest extends AbstractRequest {

    private final AddTagRequestData data;

    public static class Builder extends AbstractRequest.Builder<AddTagRequest> {
        private final AddTagRequestData data;

        public Builder(AddTagRequestData data) {
            super(ApiKeys.forId(data.apiKey()));
            this.data = data;
        }

        @Override
        public AddTagRequest build(short version) {
            return new AddTagRequest(data, version);
        }

        public AddTagRequestData data() {
            return data;
        }
    }

    public AddTagRequest(AddTagRequestData data, short version) {
        super(ApiKeys.forId(data.apiKey()), version);
        this.data = data;
    }

    @Override
    public AddTagRequestData data() {
        return data;
    }

    public static AddTagRequest parse(ByteBuffer buffer, short version) {
        return new AddTagRequest(
                new AddTagRequestData(new ByteBufferAccessor(buffer), version),
                version
        );
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new AddTagResponse(
                new AddTagResponseData()
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message())
        );
    }
}

