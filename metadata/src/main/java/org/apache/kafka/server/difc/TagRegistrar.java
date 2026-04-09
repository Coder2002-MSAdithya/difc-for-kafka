package org.apache.kafka.server.difc;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TagRegistrar
{
    private final Map<String, Tag> tagsByName = new ConcurrentHashMap<>();
    private final Map<String, ClientDIFCPrivs> clientsById = new ConcurrentHashMap<>();

    private static final int MIN_TAG_LENGTH = DIFCConstants.MIN_TAG_LENGTH;
    private static final int MIN_CLIENT_ID_LENGTH = DIFCConstants.MIN_CLIENT_ID_LENGTH;
    private static final int MAX_TAG_LENGTH = DIFCConstants.MAX_TAG_LENGTH;
    private static final int MAX_CLIENT_ID_LENGTH = DIFCConstants.MAX_CLIENT_ID_LENGTH;

    private static final String TAG_NAME_PATTERN = DIFCConstants.TAG_NAME_PATTERN;
    private static final String CLIENT_ID_NAME_PATTERN = DIFCConstants.CLIENT_NAME_PATTERN;

    public TagRegistrar() {}

    public void initialize() {}

    public static boolean isValidTagName(String tagName)
    {
        if (tagName == null) return false;
        if (tagName.length() < MIN_TAG_LENGTH || tagName.length() > MAX_TAG_LENGTH) return false;
        return tagName.matches(TAG_NAME_PATTERN);
    }

    public static boolean isValidClientId(String clientId)
    {
        if (clientId == null) return false;
        if (clientId.length() < MIN_CLIENT_ID_LENGTH || clientId.length() > MAX_CLIENT_ID_LENGTH) return false;
        return clientId.matches(CLIENT_ID_NAME_PATTERN);
    }

    public int registerClient(String clientId)
    {
        if (!isValidClientId(clientId)) return DIFCConstants.ERR_INVALID_CLIENT_ID;
        if (clientsById.containsKey(clientId)) return DIFCConstants.ERR_CLIENT_EXISTS;

        clientsById.put(clientId, new ClientDIFCPrivs(clientId));
        return DIFCConstants.OK;
    }

    private ClientDIFCPrivs getOrCreateClient(String clientId)
    {
        if (!isValidClientId(clientId)) return null;
        return clientsById.computeIfAbsent(clientId, ClientDIFCPrivs::new);
    }

    public ClientDIFCPrivs getClient(String clientId)
    {
        if (!isValidClientId(clientId)) return null;
        return clientsById.get(clientId);
    }

    public int getTagCount() { return tagsByName.size(); }
    public boolean hasTag(String tagName) { return isValidTagName(tagName) && tagsByName.containsKey(tagName); }
    public int getClientCount() { return clientsById.size(); }

    public Set<String> getTagsForClient(String clientId)
    {
        ClientDIFCPrivs client = getClient(clientId);
        return client == null ? Collections.emptySet() : Collections.unmodifiableSet(client.getTags());
    }

    public Set<String> getPositiveCapacityTagsForClient(String clientId)
    {
        ClientDIFCPrivs client = clientsById.get(clientId);
        return client == null ? Collections.emptySet() : Collections.unmodifiableSet(client.getAddCapabilities());
    }

    public Set<String> getNegativeCapacityTagsForClient(String clientId)
    {
        ClientDIFCPrivs client = clientsById.get(clientId);
        return client == null ? Collections.emptySet() : Collections.unmodifiableSet(client.getRemoveCapabilities());
    }

    public Set<String> getOwnedTagsForClient(String clientId)
    {
        ClientDIFCPrivs client = clientsById.get(clientId);
        return client == null ? Collections.emptySet() : Collections.unmodifiableSet(client.getOwnedTags());
    }

    public int createTag(String tagName, String owner)
    {
        if (!isValidTagName(tagName)) return DIFCConstants.ERR_INVALID_TAG_NAME;
        if (!isValidClientId(owner)) return DIFCConstants.ERR_INVALID_CLIENT_ID;
        if (tagsByName.containsKey(tagName)) return DIFCConstants.ERR_DUPLICATE_TAG;

        ClientDIFCPrivs ownerPrivs = clientsById.get(owner);
        if (ownerPrivs == null) return DIFCConstants.ERR_CLIENT_NOT_FOUND;

        Tag newTag = new Tag(tagName, owner);
        tagsByName.put(tagName, newTag);
        ownerPrivs.addOwnership(tagName);

        return newTag.tagId; // Returns > 0 on success
    }

    public int destroyTag(String tagName, String clientId)
    {
        if (!isValidTagName(tagName)) return DIFCConstants.ERR_INVALID_TAG_NAME;
        if (!isValidClientId(clientId)) return DIFCConstants.ERR_INVALID_CLIENT_ID;

        Tag tag = tagsByName.get(tagName);
        if (tag == null) return DIFCConstants.ERR_TAG_NOT_FOUND;

        ClientDIFCPrivs client = clientsById.get(clientId);
        if (client == null) return DIFCConstants.ERR_CLIENT_NOT_FOUND;
        if (!client.owns(tagName)) return DIFCConstants.ERR_UNAUTHORIZED;

        tagsByName.remove(tagName);
        for (ClientDIFCPrivs c : clientsById.values())
        {
            c.removeTag(tagName);
            c.removeCapability(tagName, Capability.CAN_ADD);
            c.removeCapability(tagName, Capability.CAN_REMOVE);
            c.removeOwnership(tagName);
        }
        return DIFCConstants.OK;
    }

    public ClientDIFCPrivs getOwner(String tagName)
    {
        return getClient(tagsByName.get(tagName).ownerClient);
    }

    public int getTag(String tagName)
    {
        Tag tag = tagsByName.get(tagName);
        return tag != null ? tag.tagId : DIFCConstants.ERR_TAG_NOT_FOUND;
    }

    public boolean isTagNameAvailable(String tagName)
    {
        return isValidTagName(tagName) && !tagsByName.containsKey(tagName);
    }

    public int addTag(String tagName, String clientId)
    {
        ClientDIFCPrivs client = getClient(clientId);
        if (client == null) return DIFCConstants.ERR_CLIENT_NOT_FOUND;
        if (!tagsByName.containsKey(tagName)) return DIFCConstants.ERR_TAG_NOT_FOUND;

        if (client.getAddCapabilities().contains(tagName))
        {
            return client.addTag(tagName);
        }
        return DIFCConstants.ERR_UNAUTHORIZED;
    }

    public int removeTag(String tagName, String clientId)
    {
        ClientDIFCPrivs client = getClient(clientId);
        if (client == null) return DIFCConstants.ERR_CLIENT_NOT_FOUND;
        if (!tagsByName.containsKey(tagName)) return DIFCConstants.ERR_TAG_NOT_FOUND;

        if (client.getRemoveCapabilities().contains(tagName))
        {
            return client.removeTag(tagName);
        }
        return DIFCConstants.ERR_UNAUTHORIZED;
    }

    public int addClientPrivs(String clientId, String tagName, Capability cap)
    {
        if (cap == null) return DIFCConstants.ERR_UNSUPPORTED_CAPABILITY;

        ClientDIFCPrivs client = getClient(clientId);
        if (client == null) return DIFCConstants.ERR_CLIENT_NOT_FOUND;
        if (!tagsByName.containsKey(tagName)) return DIFCConstants.ERR_TAG_NOT_FOUND;

        return client.addCapability(tagName, cap);
    }

    public int removeClientPrivs(String clientId, String tagName, Capability cap)
    {
        if (cap == null) return DIFCConstants.ERR_UNSUPPORTED_CAPABILITY;

        ClientDIFCPrivs client = getClient(clientId);
        if (client == null) return DIFCConstants.ERR_CLIENT_NOT_FOUND;
        if (!tagsByName.containsKey(tagName)) return DIFCConstants.ERR_TAG_NOT_FOUND;

        return client.removeCapability(tagName, cap);
    }

    public int addClientPrivsOnRequest(String fromClientId, String clientId, String tagName, Capability cap)
    {
        ClientDIFCPrivs fromClient = getClient(fromClientId);
        if (fromClient == null) return DIFCConstants.ERR_CLIENT_NOT_FOUND;
        if (fromClientId.equals(clientId)) return DIFCConstants.ERR_UNAUTHORIZED; // Cannot grant to self

        if (!fromClient.getOwnedTags().contains(tagName)) return DIFCConstants.ERR_UNAUTHORIZED;

        return addClientPrivs(clientId, tagName, cap);
    }

    public int removeClientPrivsOnRequest(String fromClientId, String clientId, String tagName, Capability cap)
    {
        ClientDIFCPrivs fromClient = getClient(fromClientId);
        if (fromClient == null) return DIFCConstants.ERR_CLIENT_NOT_FOUND;
        if (fromClientId.equals(clientId)) return DIFCConstants.ERR_UNAUTHORIZED;

        if (!fromClient.getOwnedTags().contains(tagName)) return DIFCConstants.ERR_UNAUTHORIZED;

        return removeClientPrivs(clientId, tagName, cap);
    }

    public int grantOwnerPrivileges(String fromClientId, String clientId, String tagName)
    {
        if (!tagsByName.containsKey(tagName)) return DIFCConstants.ERR_TAG_NOT_FOUND;

        ClientDIFCPrivs ownerClient = getClient(fromClientId);
        if (ownerClient == null) return DIFCConstants.ERR_CLIENT_NOT_FOUND;

        ClientDIFCPrivs targetClient = getClient(clientId);
        if (targetClient == null) return DIFCConstants.ERR_CLIENT_NOT_FOUND;

        if (!ownerClient.getOwnedTags().contains(tagName)) return DIFCConstants.ERR_UNAUTHORIZED;

        ownerClient.removeOwnership(tagName);
        targetClient.addOwnership(tagName);

        return DIFCConstants.OK;
    }

    public boolean canClientReceive(String receiverId, Set<String> messageTags)
    {
        if (messageTags != null) {
            for (String tagName : messageTags) {
                if (!isValidTagName(tagName)) return true;
            }
        }
        ClientDIFCPrivs receiver = getOrCreateClient(receiverId);
        if (receiver == null) return false;

        return receiver.getTags().containsAll(messageTags);
    }

    /**
     * Used when the broker boots up and loads the Snapshot from disk.
     */
    public void loadFromImage(org.apache.kafka.image.DifcImage image)
    {
        this.tagsByName.clear();
        this.clientsById.clear();

        // Load Tags
        for (java.util.Map.Entry<String, String> entry : image.tags().entrySet()) {
            this.tagsByName.put(entry.getKey(), new Tag(entry.getKey(), entry.getValue()));
        }

        // Load Clients
        for (java.util.Map.Entry<String, org.apache.kafka.server.difc.ClientDIFCPrivs> entry : image.clients().entrySet()) {
            this.clientsById.put(entry.getKey(), new org.apache.kafka.server.difc.ClientDIFCPrivs(entry.getValue()));
        }
    }

    /**
     * Used when the broker is running and receives incremental updates from the Active Controller.
     */
    public void applyDelta(org.apache.kafka.image.DifcDelta delta)
    {
        // 1. Process tag deletions
        for (String destroyedTag : delta.destroyedTags()) {
            this.tagsByName.remove(destroyedTag);
        }

        // 2. Process tag creations
        for (java.util.Map.Entry<String, String> entry : delta.createdTags().entrySet()) {
            this.tagsByName.put(entry.getKey(), new Tag(entry.getKey(), entry.getValue()));
        }

        // 3. Process client updates
        for (java.util.Map.Entry<String, org.apache.kafka.server.difc.ClientDIFCPrivs> entry : delta.updatedClients().entrySet()) {
            this.clientsById.put(entry.getKey(), new org.apache.kafka.server.difc.ClientDIFCPrivs(entry.getValue()));
        }
    }

    public int enqueueCapabilityRequest(String targetClientId, String tagName, Capability cap, String fromClientId)
    {
        ClientDIFCPrivs targetClient = getClient(targetClientId);
        if (targetClient == null) return DIFCConstants.ERR_CLIENT_NOT_FOUND;

        return targetClient.addCapabilityRequest(tagName, cap, fromClientId);
    }

    public CapabilityRequest pollCapabilityRequest(String clientId)
    {
        ClientDIFCPrivs client = getClient(clientId);
        if (client == null) return null;

        return client.pollCapabilityRequest(); // Pops the front element
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("TagRegistrar{\n");
        sb.append("  Tags (").append(tagsByName.size()).append("):\n");
        for (Map.Entry<String, Tag> entry : tagsByName.entrySet()) {
            sb.append("    ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        sb.append("  Clients (").append(clientsById.size()).append("):\n");
        for (Map.Entry<String, ClientDIFCPrivs> entry : clientsById.entrySet()) {
            sb.append("    ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        sb.append("}");
        return sb.toString();
    }
}