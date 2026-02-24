package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.message.GetOwnListRequestData;
import org.apache.kafka.common.message.GetOwnListResponseData;
import org.apache.kafka.common.protocol.*;

public class GetOwnListRequest extends AbstractRequest {

    private final GetOwnListRequestData data;

    public static class Builder extends AbstractRequest.Builder<GetOwnListRequest> {

        private final GetOwnListRequestData data;

        public Builder(GetOwnListRequestData data) {
            super(ApiKeys.forId(data.apiKey())); // 112
            this.data = data;
        }

        @Override
        public GetOwnListRequest build(short version) {
            return new GetOwnListRequest(data, version);
        }

        public GetOwnListRequestData data() {
            return data;
        }
    }

    public GetOwnListRequest(GetOwnListRequestData data, short version) {
        super(ApiKeys.forId(data.apiKey()), version);
        this.data = data;
    }

    @Override
    public GetOwnListRequestData data() {
        return data;
    }

    public static GetOwnListRequest parse(ByteBuffer buffer, short version) {
        return new GetOwnListRequest(
                new GetOwnListRequestData(new ByteBufferAccessor(buffer), version),
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

        return new GetOwnListResponse(
                new GetOwnListResponseData()
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message())
        );
    }
}
