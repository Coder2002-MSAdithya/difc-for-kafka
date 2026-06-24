package org.apache.kafka.server.difc;

import java.util.Objects;

public class CapabilityRequest
{
    private final String tagName;
    private final Capability capability;
    private final String fromClientId;
    private final byte[] attestedPolicyBytes;

    public CapabilityRequest(String tagName, Capability capability, String fromClientId)
    {
        this(tagName, capability, fromClientId, null);
    }

    public CapabilityRequest(String tagName, Capability capability, String fromClientId, byte[] attestedPolicyBytes)
    {
        this.tagName = tagName;
        this.capability = capability;
        this.fromClientId = fromClientId;
        this.attestedPolicyBytes = attestedPolicyBytes;
    }

    public String getTagName() { return tagName; }
    public Capability getCapability() { return capability; }
    public String getFromClientId() { return fromClientId; }
    public byte[] attestedPolicyBytes() { return attestedPolicyBytes; }

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
        return "PendingRequest{tag='" + tagName + "', cap=" + capability + ", from='" + fromClientId
                + "', attestedPolicyBytes=" + (attestedPolicyBytes == null ? 0 : attestedPolicyBytes.length) + "}";
    }
}
