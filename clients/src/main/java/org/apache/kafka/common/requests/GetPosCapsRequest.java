package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.message.GetPosCapsRequestData;
import org.apache.kafka.common.message.GetPosCapsResponseData;
import org.apache.kafka.common.protocol.*;

public class GetPosCapsRequest extends AbstractRequest {

    private final GetPosCapsRequestData data;

    public static class Builder extends AbstractRequest.Builder<GetPosCapsRequest> {

        private final GetPosCapsRequestData data;

        public Builder(GetPosCapsRequestData data) {
            super(ApiKeys.forId(data.apiKey())); // 110
            this.data = data;
        }

        @Override
        public GetPosCapsRequest build(short version) {
            return new GetPosCapsRequest(data, version);
        }

        public GetPosCapsRequestData data() {
            return data;
        }
    }

    public GetPosCapsRequest(GetPosCapsRequestData data, short version) {
        super(ApiKeys.forId(data.apiKey()), version);
        this.data = data;
    }

    @Override
    public GetPosCapsRequestData data() {
        return data;
    }

    public static GetPosCapsRequest parse(ByteBuffer buffer, short version) {
        return new GetPosCapsRequest(
                new GetPosCapsRequestData(new ByteBufferAccessor(buffer), version),
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

        return new GetPosCapsResponse(
                new GetPosCapsResponseData()
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message())
        );
    }
}
