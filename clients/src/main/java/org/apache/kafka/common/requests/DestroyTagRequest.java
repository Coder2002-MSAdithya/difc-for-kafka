package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.message.DestroyTagRequestData;
import org.apache.kafka.common.message.DestroyTagResponseData;
import org.apache.kafka.common.protocol.*;

/**
 * Request class for the DestroyTag API.
 */
public class DestroyTagRequest extends AbstractRequest {

    private final DestroyTagRequestData data;

    public static class Builder extends AbstractRequest.Builder<DestroyTagRequest> {

        private final DestroyTagRequestData data;

        public Builder(DestroyTagRequestData data) {
            // ApiKeys entry must correspond to apiKey() in DestroyTagRequestData (102)
            super(ApiKeys.forId(data.apiKey()));
            this.data = data;
        }

        @Override
        public DestroyTagRequest build(short version) {
            return new DestroyTagRequest(data, version);
        }

        public DestroyTagRequestData data() {
            return data;
        }
    }

    public DestroyTagRequest(DestroyTagRequestData data, short version) {
        super(ApiKeys.forId(data.apiKey()), version);
        this.data = data;
    }

    @Override
    public DestroyTagRequestData data() {
        return data;
    }

    public static DestroyTagRequest parse(ByteBuffer buffer, short version) {
        return new DestroyTagRequest(
                new DestroyTagRequestData(new ByteBufferAccessor(buffer), version),
                version
        );
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new DestroyTagResponse(
                new DestroyTagResponseData()
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message())
        );
    }
}

