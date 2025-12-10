package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.message.CreateTagRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

/**
 * Request class for the CreateTag API.
 */
public class CreateTagRequest extends AbstractRequest {

    private final CreateTagRequestData data;

    public static class Builder extends AbstractRequest.Builder<CreateTagRequest>
    {

        private final CreateTagRequestData data;

        public Builder(CreateTagRequestData data)
        {
            // ApiKeys entry must correspond to apiKey() in CreateTagRequestData (101)
            super(ApiKeys.forId(data.apiKey()));
            this.data = data;
        }

        @Override
        public CreateTagRequest build(short version)
        {
            return new CreateTagRequest(data, version);
        }

        public CreateTagRequestData data()
        {
            return data;
        }
    }

    public CreateTagRequest(CreateTagRequestData data, short version)
    {
        super(ApiKeys.forId(data.apiKey()), version);
        this.data = data;
    }

    @Override
    public CreateTagRequestData data()
    {
        return data;
    }

    public void write(Writable writable, ObjectSerializationCache cache, short version)
    {
        data.write(writable, cache, version);
    }

    public int size(ObjectSerializationCache cache, short version)
    {
        return data.size(cache, version);
    }

    public static CreateTagRequest parse(ByteBuffer buffer, short version)
    {
        return new CreateTagRequest(new CreateTagRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public ApiKeys apiKey()
    {
        return ApiKeys.forId(data.apiKey());
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return null;
    }
}
