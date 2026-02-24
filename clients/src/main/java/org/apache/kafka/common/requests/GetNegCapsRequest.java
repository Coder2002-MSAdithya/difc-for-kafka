package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.message.GetNegCapsRequestData;
import org.apache.kafka.common.message.GetNegCapsResponseData;
import org.apache.kafka.common.protocol.*;

public class GetNegCapsRequest extends AbstractRequest {

    private final GetNegCapsRequestData data;

    public static class Builder extends AbstractRequest.Builder<GetNegCapsRequest> {

        private final GetNegCapsRequestData data;

        public Builder(GetNegCapsRequestData data) {
            super(ApiKeys.forId(data.apiKey())); // 111
            this.data = data;
        }

        @Override
        public GetNegCapsRequest build(short version) {
            return new GetNegCapsRequest(data, version);
        }

        public GetNegCapsRequestData data() {
            return data;
        }
    }

    public GetNegCapsRequest(GetNegCapsRequestData data, short version) {
        super(ApiKeys.forId(data.apiKey()), version);
        this.data = data;
    }

    @Override
    public GetNegCapsRequestData data() {
        return data;
    }

    public static GetNegCapsRequest parse(ByteBuffer buffer, short version) {
        return new GetNegCapsRequest(
                new GetNegCapsRequestData(new ByteBufferAccessor(buffer), version),
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

        return new GetNegCapsResponse(
                new GetNegCapsResponseData()
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message())
        );
    }
}
