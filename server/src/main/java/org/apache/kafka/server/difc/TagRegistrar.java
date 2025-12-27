package org.apache.kafka.server.difc;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TagRegistrar
{

    private final Map<String, Tag> tagsByName = new ConcurrentHashMap<>();
    private final Map<String, ClientDIFCPrivs> clientsById = new ConcurrentHashMap<>();

    private static final int MAX_TAG_LENGTH = 16;
    private static final int OK = 0;

    public TagRegistrar()
    {
        // Empty constructor initializing empty maps
    }

    public void initialize() {
        // Initialize with hardcoded clients and tags for testing

        // Create clients
        ClientDIFCPrivs c1 = new ClientDIFCPrivs("client1");
        ClientDIFCPrivs c2 = new ClientDIFCPrivs("client2");
        ClientDIFCPrivs c3 = new ClientDIFCPrivs("client3");

        clientsById.put("client1", c1);
        clientsById.put("client2", c2);
        clientsById.put("client3", c3);

        // Create tags and assign ownership
        Tag tagA = new Tag("tagA", "client1");
        tagsByName.put("tagA", tagA);
        c1.owns.add("tagA");

        Tag tagB = new Tag("tagB", "client1");
        tagsByName.put("tagB", tagB);
        c1.owns.add("tagB");

        Tag tagC = new Tag("tagC", "client2");
        tagsByName.put("tagC", tagC);
        c2.owns.add("tagC");

        Tag tagD = new Tag("tagD", "client3");
        tagsByName.put("tagD", tagD);
        c3.owns.add("tagD");

        // Assign some capabilities
        c1.canAdd.add("tagC");
        c1.canRemove.add("tagD");

        c2.canAdd.add("tagA");
        c2.canRemove.add("tagB");

        c3.canAdd.add("tagB");
        c3.canRemove.add("tagC");

        // Assign some labels
        c1.tags.add("tagA");
        c1.tags.add("tagB");
        c2.tags.add("tagC");
        c3.tags.add("tagD");
    }

    public int registerClient(String clientId)
    {
        if (clientId == null)
            throw new NullInputException("clientId must not be null");
        if (clientsById.containsKey(clientId))
            throw new ClientExistsException("Client '" + clientId + "' already exists");

        clientsById.put(clientId, new ClientDIFCPrivs(clientId));
        return OK;
    }


    private ClientDIFCPrivs getOrCreateClient(String clientId)
    {
        return clientsById.computeIfAbsent(clientId, ClientDIFCPrivs::new);
    }

    /**
     * Create a tag.
     *
     * @return positive tagId on success
     * @throws NullInputException       if tagName or owner is null
     * @throws DuplicateTagException    if tag already exists
     * @throws OwnerNotFoundException   if owner client does not exist
     * @throws InvalidTagNameException  if tagName format is invalid
     */
    public int createTag(String tagName, String owner)
    {
        if (tagName == null || owner == null)
            throw new NullInputException("tagName and owner must not be null");

        if (tagsByName.containsKey(tagName))
            throw new DuplicateTagException("Tag '" + tagName + "' already exists");

        ClientDIFCPrivs ownerPrivs = getClientPrivs(owner);
        if (ownerPrivs == null)
            throw new OwnerNotFoundException("Owner client '" + owner + "' not found");

        // Validation consistent with protocol comment: non-empty, <=16, ^[A-Za-z0-9_-]+$
        if (tagName.isEmpty())
        {
            throw new InvalidTagNameException("Tag name CANNOT be empty.\n");
        }
        else if(tagName.length() > MAX_TAG_LENGTH)
        {
            throw new InvalidTagNameException("Tag name CANNOT be longer than " + MAX_TAG_LENGTH + ".\n");
        }
        else if(!tagName.matches("^[A-Za-z0-9_-]+$"))
        {
            throw new InvalidTagNameException("Invalid tag name '" + tagName + "'.\n Tag name CAN ONLY contain alphabets, digits, underscores and hiphen characters.. \n");
        }

        Tag newTag = new Tag(tagName, owner);
        tagsByName.put(tagName, newTag);
        ownerPrivs.owns.add(tagName);
        return newTag.tagId;
    }

    public int destroyTag(String tagName)
    {
        if (tagName == null)
            throw new NullInputException("tagName must not be null");

        Tag tag = tagsByName.get(tagName);
        if (tag == null)
            throw new TagNotFoundException("Tag '" + tagName + "' not found");

        tagsByName.remove(tagName);

        // Remove from all clients' sets
        for (ClientDIFCPrivs client : clientsById.values())
        {
            client.tags.remove(tagName);
            client.canAdd.remove(tagName);
            client.canRemove.remove(tagName);
            client.owns.remove(tagName);
        }

        return OK;
    }

    public int getTag(String tagName)
    {
        Tag tag = tagsByName.get(tagName);
        return tag != null ? tag.tagId : -1;
    }

    public int addClientPrivs(String clientId, String tagName, Capability cap)
    {
        if (clientId == null || tagName == null || cap == null)
            throw new NullInputException("clientId, tagName and cap must not be null");

        if (!tagsByName.containsKey(tagName))
            throw new TagNotFoundException("Tag '" + tagName + "' not found");

        ClientDIFCPrivs client = getOrCreateClient(clientId);

        switch (cap) {
            case CAN_ADD:
                client.canAdd.add(tagName);
                break;
            case CAN_REMOVE:
                client.canRemove.add(tagName);
                break;
            default:
                throw new CapabilityException("Unsupported capability " + cap);
        }

        return 0;
    }

    public int removeClientPrivs(String clientId, String tagName, Capability cap)
    {
        if (clientId == null || tagName == null || cap == null)
            throw new NullInputException("clientId, tagName and cap must not be null");

        if (!tagsByName.containsKey(tagName))
            throw new TagNotFoundException("Tag '" + tagName + "' not found");

        ClientDIFCPrivs client = getOrCreateClient(clientId);

        switch (cap) {
            case CAN_ADD:
                client.canAdd.remove(tagName);
                break;
            case CAN_REMOVE:
                client.canRemove.remove(tagName);
                break;
            default:
                throw new CapabilityException("Unsupported capability " + cap);
        }

        return 0;
    }

    public int getTagCount()
    {
        return tagsByName.size();
    }

    public boolean hasTag(String tagName)
    {
        return tagsByName.containsKey(tagName);
    }

    public int getClientCount()
    {
        return clientsById.size();
    }

    public ClientDIFCPrivs getClientPrivs(String clientId)
    {
        return clientsById.get(clientId);
    }

    public boolean canClientReceive(String senderId, String receiverId, Set<String> messageTags)
    {
        ClientDIFCPrivs sender = getClientPrivs(senderId);
        ClientDIFCPrivs receiver = getClientPrivs(receiverId);
        if (sender == null || receiver == null) {
            return false;
        }

        Set<String> union = new HashSet<>(sender.tags);
        union.addAll(messageTags);
        return receiver.tags.containsAll(union);
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
