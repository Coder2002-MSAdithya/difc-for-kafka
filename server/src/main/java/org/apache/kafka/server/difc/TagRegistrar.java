package org.apache.kafka.server.difc;

import java.util.HashSet;
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
    private static final int OK = DIFCConstants.OK;

    public TagRegistrar()
    {
        // Empty constructor initializing empty maps
    }

    public void initialize()
    {
        // Initialize with hardcoded clients and tags for testing
        // Create clients
        ClientDIFCPrivs c1 = registerClient("client1");
        ClientDIFCPrivs c2 = registerClient("client2");
        ClientDIFCPrivs c3 = registerClient("client3");

        // Create tags and assign ownership
        createTag("tagA", "client1");
        createTag("tagB", "client1");
        createTag("tagC", "client2");
        createTag("tagD", "client3");

        // Add capabilities to the various clients
        c1.addCapability("tagC", Capability.CAN_ADD);
        c1.addCapability("tagD", Capability.CAN_REMOVE);

        c2.addCapability("tagA", Capability.CAN_ADD);
        c2.addCapability("tagB", Capability.CAN_REMOVE);

        c3.addCapability("tagB", Capability.CAN_ADD);
        c3.addCapability("tagC", Capability.CAN_REMOVE);

        c1.addTag("tagA");
        c1.addTag("tagB");
        c2.addTag("tagC");
        c3.addTag("tagD");
    }

    public ClientDIFCPrivs registerClient(String clientId)
    {
        isValidClientId(clientId);
        if (clientsById.containsKey(clientId))
            throw new ClientExistsException("Client '" + clientId + "' already exists");
        ClientDIFCPrivs newClient = new ClientDIFCPrivs(clientId);
        clientsById.put(clientId, newClient);
        return newClient;
    }

    private ClientDIFCPrivs getOrCreateClient(String clientId)
    {
        isValidClientId(clientId);
        return clientsById.computeIfAbsent(clientId, ClientDIFCPrivs::new);
    }

    public ClientDIFCPrivs getClient(String clientId)
    {
        isValidClientId(clientId);
        ClientDIFCPrivs c = clientsById.get(clientId);
        if(c == null)
            throw new ClientNotFoundException("Client '" + clientId + "' does not exist");
        return c;
    }

    public int getTagCount()
    {
        return tagsByName.size();
    }

    public boolean hasTag(String tagName)
    {
        isValidTagName(tagName);
        return tagsByName.containsKey(tagName);
    }

    public int getClientCount()
    {
        return clientsById.size();
    }

    public static void isValidTagName(String tagName)
    {
        // Validation consistent with protocol comment: non-empty, <=16, ^[A-Za-z0-9_-]+$
        if(tagName == null)
        {
            throw new NullInputException("tagName and owner must not be null");
        }
        if (tagName.isEmpty())
        {
            throw new InvalidTagNameException("Tag name CANNOT be empty.\n");
        }
        else if(tagName.length() < MIN_TAG_LENGTH)
        {
            throw new InvalidTagNameException("Tag name length must be at least " + MIN_TAG_LENGTH + ".\n");
        }
        else if(tagName.length() > MAX_TAG_LENGTH)
        {
            throw new InvalidTagNameException("Tag name CANNOT be longer than " + MAX_TAG_LENGTH + ".\n");
        }
        else if(!tagName.matches(TAG_NAME_PATTERN))
        {
            throw new InvalidTagNameException("Invalid tag name '" + tagName + "'.\n Tag name CAN ONLY contain alphabets, digits, underscores and hiphen characters.. \n");
        }
     }

     public static void isValidClientId(String clientId)
     {
         if(clientId == null)
         {
             throw new NullInputException("clientId must not be null");
         }
         else if(clientId.isEmpty())
         {
             throw new InvalidClientIdException("Client ID must not be empty.\n");
         }
         else if(clientId.length() < MIN_CLIENT_ID_LENGTH)
         {
             throw new InvalidClientIdException("Client ID must be at least " + MIN_CLIENT_ID_LENGTH + ".\n");
         }
         else if(clientId.length() > MAX_CLIENT_ID_LENGTH)
         {
             throw new InvalidClientIdException("Client ID can't be longer than " + MAX_CLIENT_ID_LENGTH + ".\n");
         }
         else if(!clientId.matches(CLIENT_ID_NAME_PATTERN))
         {
             throw new InvalidClientIdException("Invalid client ID '" + clientId + "'. Client ID CAN ONLY contain alphabets, digits, underscores and hiphen characters.. \n");
         }
     }

    /**
     * Create a tag.
     *
     * @return positive tagId on success
     * @throws NullInputException       if tagName or owner is null
     * @throws DuplicateTagException    if tag already exists
     * @throws ClientNotFoundException   if owner client id does not exist
     * @throws InvalidTagNameException  if tagName format is invalid
     */
    public int createTag(String tagName, String owner)
    {
        isValidTagName(tagName);
        isValidClientId(owner);

        if(tagsByName.containsKey(tagName))
            throw new DuplicateTagException("Tag '" + tagName + "' already exists");

        ClientDIFCPrivs ownerPrivs = getClient(owner);

        Tag newTag = new Tag(tagName, owner);
        tagsByName.put(tagName, newTag);
        ownerPrivs.addOwnership(tagName);
        return newTag.tagId;
    }

    public int destroyTag(String tagName, String clientId)
    {
        isValidTagName(tagName);
        isValidClientId(clientId);

        Tag tag = tagsByName.get(tagName);
        if (tag == null)
            throw new TagNotFoundException("Tag '" + tagName + "' not found");

        if(!getClient(clientId).owns(tagName))
            throw new UnAuthorizedClientException("ONLY owners of tags CAN destroy them, " + clientId + " thus unauthorized to perform this operation");

        tagsByName.remove(tagName);

        // Remove from all clients' sets
        for (ClientDIFCPrivs client : clientsById.values())
        {
            client.removeTag(tagName);
            client.removeCapability(tagName, Capability.CAN_ADD);
            client.removeCapability(tagName, Capability.CAN_REMOVE);
            client.removeOwnership(tagName);
        }

        return OK;
    }

    public int getTag(String tagName)
    {
        Tag tag = tagsByName.get(tagName);
        return tag != null ? tag.tagId : -1;
    }

    public boolean isTagNameAvailable(String tagName)
    {
        isValidTagName(tagName);
        return !tagsByName.containsKey(tagName);
    }

    public int addTag(String tagName, String clientId)
    {
        ClientDIFCPrivs client = getOrCreateClient(clientId);
        if(client.getAddCapabilities().contains(tagName))
        {
            client.addTag(tagName);
            return OK;
        }
        throw new UnAuthorizedClientException("Client  '" + clientId + "' is unauthorized to perform this operation.");
    }

    public int removeTag(String tagName, String clientId)
    {
        ClientDIFCPrivs client = getOrCreateClient(clientId);

        if(client.getRemoveCapabilities().contains(tagName))
        {
            client.removeTag(tagName);
            return OK;
        }

        throw new UnAuthorizedClientException("Client  '" + clientId + "' is unauthorized to perform this operation.");
    }

    public int addClientPrivs(String clientId, String tagName, Capability cap)
    {
        isValidClientId(clientId);
        isValidTagName(tagName);
        if(!tagsByName.containsKey(tagName))
            throw new TagNotFoundException("Tag '" + tagName + "' not found");
        ClientDIFCPrivs client = getOrCreateClient(clientId);
        client.addCapability(tagName, cap);
        return OK;
    }

    public int removeClientPrivs(String clientId, String tagName, Capability cap)
    {
        if (clientId == null || tagName == null || cap == null)
            throw new NullInputException("clientId, tagName and cap must not be null");
        if(!tagsByName.containsKey(tagName))
            throw new TagNotFoundException("Tag '" + tagName + "' not found");
        ClientDIFCPrivs client = getOrCreateClient(clientId);
        client.removeCapability(tagName, cap);
        return OK;
    }

    public int addClientPrivsOnRequest(String fromClientId, String clientId, String tagName, Capability cap)
    {
        isValidClientId(fromClientId);
        isValidClientId(clientId);

        if(clientId.equals(fromClientId))
            throw new TagException("CANNOT add privileges to yourself through this method.");

        if(!getClient(fromClientId).getOwnedTags().contains(tagName))
        {
            throw new UnAuthorizedClientException("Client '" + fromClientId + "' does NOT own this tag to bestow capabilities to another client..");
        }

        return addClientPrivs(clientId, tagName, cap);
    }

    public int removeClientPrivsOnRequest(String fromClientId, String clientId, String tagName, Capability cap)
    {
        isValidClientId(fromClientId);
        isValidClientId(clientId);

        if(clientId.equals(fromClientId))
            throw new TagException("CANNOT remove privileges from yourself through this method.");

        if(!getClient(fromClientId).getOwnedTags().contains(tagName))
        {
            throw new UnAuthorizedClientException("Client '" + fromClientId + "' does NOT own this tag to bestow capabilities to another client..");
        }

        return removeClientPrivs(clientId, tagName, cap);
    }

    public int grantOwnerPrivileges(String fromClientId, String clientId, String tagName)
    {
        isValidClientId(fromClientId);
        isValidClientId(clientId);
        isValidTagName(tagName);

        if(!tagsByName.containsKey(tagName))
            throw new TagNotFoundException("Tag '" + tagName + "' not found");

        ClientDIFCPrivs ownerClient = getClient(fromClientId);
        ClientDIFCPrivs client = getOrCreateClient(clientId);

        if(!ownerClient.getOwnedTags().contains(tagName))
            throw new UnAuthorizedClientException("Client '" + clientId + "' is unauthorized to perform this operation as you do NOT own this tag.");

        ownerClient.removeOwnership(tagName);
        client.addOwnership(tagName);

        return OK;
    }

    public boolean canClientReceive(String senderId, String receiverId, Set<String> messageTags)
    {
        isValidClientId(senderId);
        isValidClientId(receiverId);

        for(String tagName : messageTags)
        {
            isValidTagName(tagName);
        }

        ClientDIFCPrivs sender = getClient(senderId);
        ClientDIFCPrivs receiver = getClient(receiverId);

        Set<String> union = new HashSet<>(sender.getTags());
        union.addAll(messageTags);
        return receiver.getTags().containsAll(union);
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