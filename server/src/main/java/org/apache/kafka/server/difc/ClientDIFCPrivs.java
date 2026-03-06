package org.apache.kafka.server.difc;
import org.apache.kafka.server.difc.exceptions.CapabilityException;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ClientDIFCPrivs
{
    private final String clientId;
    private final Set<String> tags; // current label of the DIFC client
    private final Set<String> canAdd;
    private final Set<String> canRemove;
    private final Set<String> owns;

    public ClientDIFCPrivs(String clientId)
    {
        TagRegistrar.isValidClientId(clientId);
        this.clientId = clientId;
        this.tags = ConcurrentHashMap.newKeySet();
        this.canAdd = ConcurrentHashMap.newKeySet();
        this.canRemove = ConcurrentHashMap.newKeySet();
        this.owns = ConcurrentHashMap.newKeySet();
    }

    public boolean owns(String tagName)
    {
        TagRegistrar.isValidTagName(tagName);
        return owns.contains(tagName);
    }

    public boolean canAdd(String tagName)
    {
        TagRegistrar.isValidTagName(tagName);
        return canAdd.contains(tagName);
    }

    public boolean canRemove(String tagName)
    {
        TagRegistrar.isValidTagName(tagName);
        return canRemove.contains(tagName);
    }

    public String getClientId()
    {
        return clientId;
    }

    public void addTag(String tagName)
    {
        TagRegistrar.isValidTagName(tagName);
        tags.add(tagName);
    }

    public void removeTag(String tagName)
    {
        TagRegistrar.isValidTagName(tagName);
        tags.remove(tagName);
    }

    public void addCapability(String tagName, Capability cap)
    {
        TagRegistrar.isValidTagName(tagName);
        switch (cap)
        {
            case CAN_ADD:
                canAdd.add(tagName);
                break;
            case CAN_REMOVE:
                canRemove.add(tagName);
                break;
            default:
                throw new CapabilityException("Unsupported capability " + cap);
        }
    }

    public void removeCapability(String tagName, Capability cap)
    {
        TagRegistrar.isValidTagName(tagName);
        switch(cap)
        {
            case CAN_ADD:
                canAdd.remove(tagName);
                break;
            case CAN_REMOVE:
                canRemove.remove(tagName);
                break;
            default:
                throw new CapabilityException("Unsupported capability " + cap);
        }
    }

    public void addOwnership(String tagName)
    {
        TagRegistrar.isValidTagName(tagName);
        owns.add(tagName);
    }

    public void removeOwnership(String tagName)
    {
        TagRegistrar.isValidTagName(tagName);
        owns.remove(tagName);
    }

    public Set<String> getTags()
    {
        return Collections.unmodifiableSet(tags);
    }

    public Set<String> getAddCapabilities()
    {
        Set<String> caps = ConcurrentHashMap.newKeySet();
        caps.addAll(canAdd);
        caps.addAll(owns);
        return Collections.unmodifiableSet(caps);
    }

    public Set<String> getRemoveCapabilities()
    {
        Set<String> caps = ConcurrentHashMap.newKeySet();
        caps.addAll(canRemove);
        caps.addAll(owns);
        return Collections.unmodifiableSet(caps);
    }

    public Set<String> getOwnedTags()
    {
        return Collections.unmodifiableSet(owns);
    }

    @Override
    public String toString()
    {
        return "ClientDIFCPrivs{" +
                "clientId='" + clientId + '\'' +
                ", tags=" + tags +
                ", canAdd=" + canAdd +
                ", canRemove=" + canRemove +
                ", owns=" + owns +
                '}';
    }
}