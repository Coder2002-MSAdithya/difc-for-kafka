package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.message.GetLabelRequestData;
import org.apache.kafka.common.message.GetLabelResponseData;
import org.apache.kafka.common.protocol.*;

public class GetLabelRequest extends AbstractRequest {

    private final GetLabelRequestData data;

    public static class Builder extends AbstractRequest.Builder<GetLabelRequest> {

        private final GetLabelRequestData data;

        public Builder(GetLabelRequestData data) {
            super(ApiKeys.forId(data.apiKey())); // 109
            this.data = data;
        }

        @Override
        public GetLabelRequest build(short version) {
            return new GetLabelRequest(data, version);
        }

        public GetLabelRequestData data() {
            return data;
        }
    }

    public GetLabelRequest(GetLabelRequestData data, short version) {
        super(ApiKeys.forId(data.apiKey()), version);
        this.data = data;
    }

    @Override
    public GetLabelRequestData data() {
        return data;
    }

    public static GetLabelRequest parse(ByteBuffer buffer, short version) {
        return new GetLabelRequest(
                new GetLabelRequestData(new ByteBufferAccessor(buffer), version),
                version
        );
    }

    @Override
    public ApiKeys apiKey() {
        return ApiKeys.forId(data.apiKey());
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new GetLabelResponse(
                new GetLabelResponseData()
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message())
        );
    }
}
