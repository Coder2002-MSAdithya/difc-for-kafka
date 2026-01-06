package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.message.GrantOwnerPrivilegesRequestData;
import org.apache.kafka.common.message.GrantOwnerPrivilegesResponseData;
import org.apache.kafka.common.protocol.*;

public class GrantOwnerPrivilegesRequest extends AbstractRequest {

    private final GrantOwnerPrivilegesRequestData data;

    public static class Builder extends AbstractRequest.Builder<GrantOwnerPrivilegesRequest> {
        private final GrantOwnerPrivilegesRequestData data;

        public Builder(GrantOwnerPrivilegesRequestData data) {
            super(ApiKeys.forId(data.apiKey()));
            this.data = data;
        }

        @Override
        public GrantOwnerPrivilegesRequest build(short version) {
            return new GrantOwnerPrivilegesRequest(data, version);
        }
    }

    public GrantOwnerPrivilegesRequest(GrantOwnerPrivilegesRequestData data, short version) {
        super(ApiKeys.forId(data.apiKey()), version);
        this.data = data;
    }

    @Override
    public GrantOwnerPrivilegesRequestData data() {
        return data;
    }

    public static GrantOwnerPrivilegesRequest parse(ByteBuffer buffer, short version) {
        return new GrantOwnerPrivilegesRequest(
                new GrantOwnerPrivilegesRequestData(new ByteBufferAccessor(buffer), version),
                version
        );
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new GrantOwnerPrivilegesResponse(
                new GrantOwnerPrivilegesResponseData()
                        .setErrorCode(error.code())
                        .setErrorMessage(error.message())
        );
    }
}
