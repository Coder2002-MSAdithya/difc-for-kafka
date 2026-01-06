package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.message.AddClientPrivsRequestData;
import org.apache.kafka.common.message.AddClientPrivsResponseData;
import org.apache.kafka.common.protocol.*;

public class AddClientPrivsRequest extends AbstractRequest {

    private final AddClientPrivsRequestData data;

    public static class Builder extends AbstractRequest.Builder<AddClientPrivsRequest> {
        private final AddClientPrivsRequestData data;

        public Builder(AddClientPrivsRequestData data) {
            super(ApiKeys.forId(data.apiKey()));
            this.data = data;
        }

        @Override
        public AddClientPrivsRequest build(short version) {
            return new AddClientPrivsRequest(data, version);
        }
    }

    public AddClientPrivsRequest(AddClientPrivsRequestData data, short version) {
        super(ApiKeys.forId(data.apiKey()), version);
        this.data = data;
    }

    @Override
    public AddClientPrivsRequestData data() {
        return data;
    }

    public static AddClientPrivsRequest parse(ByteBuffer buffer, short version) {
        return new AddClientPrivsRequest(
                new AddClientPrivsRequestData(new ByteBufferAccessor(buffer), version),
                version
        );
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new AddClientPrivsResponse(
                new AddClientPrivsResponseData()
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message())
        );
    }
}

