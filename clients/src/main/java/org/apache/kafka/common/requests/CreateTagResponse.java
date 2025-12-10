package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.message.CreateTagResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Writable;

public class CreateTagResponse extends AbstractResponse
{
    private final CreateTagResponseData data;

    public CreateTagResponse(CreateTagResponseData data)
    {
        super(ApiKeys.forId(data.apiKey()));  // important: call AbstractResponse(ApiKeys)
        this.data = data;
    }

    public CreateTagResponseData data()
    {
        return data;
    }

    public static CreateTagResponse parse(ByteBuffer buffer, short version)
    {
        return new CreateTagResponse(
                new CreateTagResponseData(new ByteBufferAccessor(buffer), version)
        );
    }

    public void write(Writable writable, ObjectSerializationCache cache, short version)
    {
        data.write(writable, cache, version);
    }

    public int size(ObjectSerializationCache cache, short version)
    {
        return data.size(cache, version);
    }

    @Override
    public Map<Errors, Integer> errorCounts()
    {
        // Single errorCode field on the response
        Errors error = Errors.forCode(data.errorCode());
        if (error == Errors.NONE) {
            return Collections.emptyMap();
        }
        return Collections.singletonMap(error, 1);
    }

    @Override
    public ApiKeys apiKey()
    {
        return ApiKeys.forId(data.apiKey());
    }

    @Override
    public int throttleTimeMs()
    {
        // No throttle field in your schema
        return 0;
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs)
    {
        // No-op because there is no throttle field to set
    }

    @Override
    public String toString()
    {
        return data.toString();
    }
}
