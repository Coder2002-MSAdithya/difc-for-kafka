package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.message.GrantCapRequestData;
import org.apache.kafka.common.message.GrantCapResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

public class GrantCapRequest extends AbstractRequest {

    private final GrantCapRequestData data;

    public static class Builder extends AbstractRequest.Builder<GrantCapRequest> {
        private final GrantCapRequestData data;

        public Builder(GrantCapRequestData data) {
            super(ApiKeys.forId(data.apiKey()));
            this.data = data;
        }

        @Override
        public GrantCapRequest build(short version) {
            return new GrantCapRequest(data, version);
        }

        public GrantCapRequestData data() {
            return data;
        }
    }

    public GrantCapRequest(GrantCapRequestData data, short version) {
        super(ApiKeys.forId(data.apiKey()), version);
        this.data = data;
    }

    @Override
    public GrantCapRequestData data() {
        return data;
    }

    public static GrantCapRequest parse(ByteBuffer buffer, short version) {
        return new GrantCapRequest(
                new GrantCapRequestData(new ByteBufferAccessor(buffer), version),
                version
        );
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new GrantCapResponse(
                new GrantCapResponseData()
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message())
        );
    }
}