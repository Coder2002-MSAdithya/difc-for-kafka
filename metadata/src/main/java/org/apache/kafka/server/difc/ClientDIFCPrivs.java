package org.apache.kafka.server.difc;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ClientDIFCPrivs
{
    private final String clientId;
    private final Set<String> tags;
    private final Set<String> canAdd;
    private final Set<String> canRemove;
    private final Set<String> owns;

    private final ConcurrentLinkedQueue<CapabilityRequest> pendingRequests;

    public ClientDIFCPrivs(String clientId)
    {
        this.clientId = clientId;
        this.tags = ConcurrentHashMap.newKeySet();
        this.canAdd = ConcurrentHashMap.newKeySet();
        this.canRemove = ConcurrentHashMap.newKeySet();
        this.owns = ConcurrentHashMap.newKeySet();
        this.pendingRequests = new ConcurrentLinkedQueue<>();
    }

    // COPY CONSTRUCTOR FOR DELTA MUTATIONS
    public ClientDIFCPrivs(ClientDIFCPrivs other)
    {
        this.clientId = other.clientId;
        this.tags = ConcurrentHashMap.newKeySet();
        this.tags.addAll(other.tags);
        this.canAdd = ConcurrentHashMap.newKeySet();
        this.canAdd.addAll(other.canAdd);
        this.canRemove = ConcurrentHashMap.newKeySet();
        this.canRemove.addAll(other.canRemove);
        this.owns = ConcurrentHashMap.newKeySet();
        this.owns.addAll(other.owns);
        this.pendingRequests = new ConcurrentLinkedQueue<>(other.pendingRequests);
    }

    public boolean owns(String tagName)
    {
        if (!TagRegistrar.isValidTagName(tagName)) return false;
        return owns.contains(tagName);
    }

    public boolean canAdd(String tagName)
    {
        if (!TagRegistrar.isValidTagName(tagName)) return false;
        return canAdd.contains(tagName);
    }

    public boolean canRemove(String tagName)
    {
        if (!TagRegistrar.isValidTagName(tagName)) return false;
        return canRemove.contains(tagName);
    }

    public String getClientId()
    {
        return clientId;
    }

    public int addTag(String tagName)
    {
        if (!TagRegistrar.isValidTagName(tagName)) return DIFCConstants.ERR_INVALID_TAG_NAME;
        tags.add(tagName);
        return DIFCConstants.OK;
    }

    public int removeTag(String tagName)
    {
        if (!TagRegistrar.isValidTagName(tagName)) return DIFCConstants.ERR_INVALID_TAG_NAME;
        tags.remove(tagName);
        return DIFCConstants.OK;
    }

    public int addCapability(String tagName, Capability cap)
    {
        if (!TagRegistrar.isValidTagName(tagName)) return DIFCConstants.ERR_INVALID_TAG_NAME;
        if (cap == null) return DIFCConstants.ERR_UNSUPPORTED_CAPABILITY;

        switch (cap) {
            case CAN_ADD: canAdd.add(tagName); break;
            case CAN_REMOVE: canRemove.add(tagName); break;
            default: return DIFCConstants.ERR_UNSUPPORTED_CAPABILITY;
        }
        return DIFCConstants.OK;
    }

    public int removeCapability(String tagName, Capability cap)
    {
        if (!TagRegistrar.isValidTagName(tagName)) return DIFCConstants.ERR_INVALID_TAG_NAME;
        if (cap == null) return DIFCConstants.ERR_UNSUPPORTED_CAPABILITY;

        switch (cap) {
            case CAN_ADD: canAdd.remove(tagName); break;
            case CAN_REMOVE: canRemove.remove(tagName); break;
            default: return DIFCConstants.ERR_UNSUPPORTED_CAPABILITY;
        }
        return DIFCConstants.OK;
    }

    public int addOwnership(String tagName)
    {
        if (!TagRegistrar.isValidTagName(tagName)) return DIFCConstants.ERR_INVALID_TAG_NAME;
        owns.add(tagName);
        return DIFCConstants.OK;
    }

    public int removeOwnership(String tagName)
    {
        if (!TagRegistrar.isValidTagName(tagName)) return DIFCConstants.ERR_INVALID_TAG_NAME;
        owns.remove(tagName);
        return DIFCConstants.OK;
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

    public int addCapabilityRequest(String tagName, Capability cap, String fromClientId)
    {
        return addCapabilityRequest(tagName, cap, fromClientId, null);
    }

    public int addCapabilityRequest(String tagName, Capability cap, String fromClientId, byte[] attestedPolicyBytes)
    {
        CapabilityRequest newReq = new CapabilityRequest(tagName, cap, fromClientId, attestedPolicyBytes);

        // Prevent duplicate spam, but add to the back of the queue
        if (!pendingRequests.contains(newReq)) {
            pendingRequests.offer(newReq);
        }
        return DIFCConstants.OK;
    }

    /**
     * Retrieves AND REMOVES the front-most pending request.
     */
    public CapabilityRequest pollCapabilityRequest()
    {
        return pendingRequests.poll();
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