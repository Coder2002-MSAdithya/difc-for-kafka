package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.message.RegisterClientRequestData;
import org.apache.kafka.common.message.RegisterClientResponseData;
import org.apache.kafka.common.protocol.*;

/**
 * Request class for the RegisterClient API.
 */
public class RegisterClientRequest extends AbstractRequest {

    private final RegisterClientRequestData data;

    public static class Builder extends AbstractRequest.Builder<RegisterClientRequest> {

        private final RegisterClientRequestData data;

        public Builder(RegisterClientRequestData data) {
            // ApiKeys entry must correspond to apiKey() in RegisterClientRequestData (103)
            super(ApiKeys.forId(data.apiKey()));
            this.data = data;
        }

        @Override
        public RegisterClientRequest build(short version) {
            return new RegisterClientRequest(data, version);
        }

        public RegisterClientRequestData data() {
            return data;
        }
    }

    public RegisterClientRequest(RegisterClientRequestData data, short version) {
        super(ApiKeys.forId(data.apiKey()), version);
        this.data = data;
    }

    @Override
    public RegisterClientRequestData data() {
        return data;
    }

    public static RegisterClientRequest parse(ByteBuffer buffer, short version) {
        return new RegisterClientRequest(
                new RegisterClientRequestData(new ByteBufferAccessor(buffer), version),
                version
        );
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new RegisterClientResponse(
                new RegisterClientResponseData()
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message())
        );
    }
}

