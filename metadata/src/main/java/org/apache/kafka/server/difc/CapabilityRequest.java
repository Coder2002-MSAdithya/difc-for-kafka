package org.apache.kafka.server.difc;

import java.util.Objects;

public class CapabilityRequest
{
    private final String tagName;
    private final Capability capability;
    private final String fromClientId;

    public CapabilityRequest(String tagName, Capability capability, String fromClientId)
    {
        this.tagName = tagName;
        this.capability = capability;
        this.fromClientId = fromClientId;
    }

    public String getTagName() { return tagName; }
    public Capability getCapability() { return capability; }
    public String getFromClientId() { return fromClientId; }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CapabilityRequest that = (CapabilityRequest) o;
        return tagName.equals(that.tagName) &&
                capability == that.capability &&
                fromClientId.equals(that.fromClientId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tagName, capability, fromClientId);
    }

    @Override
    public String toString()
    {
        return "PendingRequest{tag='" + tagName + "', cap=" + capability + ", from='" + fromClientId + "'}";
    }
}
