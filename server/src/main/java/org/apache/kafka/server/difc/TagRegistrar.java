package org.apache.kafka.server.difc;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TagRegistrar
{
    private final Map<String, Tag> tagsByName = new ConcurrentHashMap<>();
    private final Map<String, ClientDIFCPrivs> clientsById = new ConcurrentHashMap<>();

    public TagRegistrar()
    {
        // Empty constructor initializing empty maps
    }

    public void initialize()
    {
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
        if (clientId == null) return -1;
        if (clientsById.containsKey(clientId)) return -1;
        clientsById.put(clientId, new ClientDIFCPrivs(clientId));
        return 0;
    }

    private ClientDIFCPrivs getOrCreateClient(String clientId)
    {
        return clientsById.computeIfAbsent(clientId, ClientDIFCPrivs::new);
    }

    public int createTag(String tagName, String owner)
    {
        if (tagName == null || owner == null) return -1;
        if (tagsByName.containsKey(tagName)) return -1;
        ClientDIFCPrivs ownerPrivs = getClientPrivs(owner);
        if (ownerPrivs == null) return -1;
        try
        {
            Tag newTag = new Tag(tagName, owner);
            tagsByName.put(tagName, newTag);
            ownerPrivs.owns.add(tagName);
            return newTag.tagId;
        }
        catch(IllegalArgumentException e)
        {
            return -1;
        }
    }


    public int destroyTag(String tagName)
    {
        Tag tag = tagsByName.get(tagName);
        if (tag == null) return -1;
        tagsByName.remove(tagName);
        // Remove from all clients' sets
        for (ClientDIFCPrivs client : clientsById.values())
        {
            client.tags.remove(tagName);
            client.canAdd.remove(tagName);
            client.canRemove.remove(tagName);
            client.owns.remove(tagName);
        }
        return 0;
    }

    public int getTag(String tagName)
    {
        Tag tag = tagsByName.get(tagName);
        return tag != null ? tag.tagId : -1;
    }

    public int addClientPrivs(String clientId, String tagName, Capability cap)
    {
        if (clientId == null || tagName == null || cap == null) return -1;
        if (!tagsByName.containsKey(tagName)) return -1;
        ClientDIFCPrivs client = getOrCreateClient(clientId);
        switch (cap)
        {
            case CAN_ADD:
                client.canAdd.add(tagName);
                break;
            case CAN_REMOVE:
                client.canRemove.add(tagName);
                break;
            default:
                return -1;
        }
        return 0;
    }

    public int removeClientPrivs(String clientId, String tagName, Capability cap)
    {
        if (clientId == null || tagName == null || cap == null) return -1;
        if (!tagsByName.containsKey(tagName)) return -1;
        ClientDIFCPrivs client = getOrCreateClient(clientId);
        switch (cap)
        {
            case CAN_ADD:
                client.canAdd.remove(tagName);
                break;
            case CAN_REMOVE:
                client.canRemove.remove(tagName);
                break;
            default:
                return -1;
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
        if (sender == null || receiver == null)
        {
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
        for (Map.Entry<String, Tag> entry : tagsByName.entrySet())
        {
            sb.append("    ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        sb.append("  Clients (").append(clientsById.size()).append("):\n");
        for (Map.Entry<String, ClientDIFCPrivs> entry : clientsById.entrySet())
        {
            sb.append("    ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        sb.append("}");
        return sb.toString();
    }
}