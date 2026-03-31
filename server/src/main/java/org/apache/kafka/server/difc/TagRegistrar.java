package org.apache.kafka.server.difc;
import org.apache.kafka.server.difc.exceptions.*;

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
    private static final int OK = DIFCConstants.OK;

    public TagRegistrar()
    {
        // Empty constructor initializing empty maps
    }

    public void initialize()
    {
        /*
         * DIFC setup for a food-delivery platform.
         *
         * Clients represent backend services:
         *   - customerApp: order creation
         *   - restaurantSvc: food preparation
         *   - deliverySvc: delivery logistics
         *   - paymentSvc: payments & refunds
         *   - supportSvc: customer support & issue resolution
         *   - auditSvc: compliance, fraud detection, logging
         */

        // ---- 1. Register clients (system services) ----
        //ClientDIFCPrivs customerApp   = registerClient("customerApp");
        //ClientDIFCPrivs restaurantSvc = registerClient("restaurantSvc");
        //ClientDIFCPrivs deliverySvc   = registerClient("deliverySvc");
        //ClientDIFCPrivs paymentSvc    = registerClient("paymentSvc");
        //ClientDIFCPrivs supportSvc    = registerClient("supportSvc");
        //ClientDIFCPrivs auditSvc      = registerClient("auditSvc");

        // ---- 2. Create DIFC tags (data classifications / order states) ----
        // Ownership reflects which service governs that stage of the order lifecycle
        //createTag("ORDER_PLACED",     "customerApp");
        //createTag("FOOD_PREPARED",    "restaurantSvc");
        //createTag("OUT_FOR_DELIVERY", "deliverySvc");
        //createTag("PAYMENT_CAPTURED","paymentSvc");
        //createTag("REFUND_ISSUED",    "paymentSvc");
        //createTag("AUDIT_LOG",        "auditSvc");

        // ---- 3. Grant cross-service capabilities ----
        // Customer app can create new orders
        //customerApp.addCapability("ORDER_PLACED", Capability.CAN_ADD);

        // Restaurant can mark food as prepared
        //restaurantSvc.addCapability("FOOD_PREPARED", Capability.CAN_ADD);
        //restaurantSvc.addCapability("FOOD_PREPARED", Capability.CAN_REMOVE);

        // Delivery service manages delivery status
        //deliverySvc.addCapability("OUT_FOR_DELIVERY", Capability.CAN_ADD);
        //deliverySvc.addCapability("OUT_FOR_DELIVERY", Capability.CAN_REMOVE);

        // Payment service manages payment and refunds
        //paymentSvc.addCapability("PAYMENT_CAPTURED", Capability.CAN_ADD);
        //paymentSvc.addCapability("PAYMENT_CAPTURED", Capability.CAN_REMOVE);
        //paymentSvc.addCapability("REFUND_ISSUED", Capability.CAN_ADD);

        // Support can request refunds but cannot remove payment evidence
        //supportSvc.addCapability("REFUND_ISSUED", Capability.CAN_ADD);

        // Audit can only add audit logs
        //auditSvc.addCapability("AUDIT_LOG", Capability.CAN_ADD);

        // ---- 4. Initial security labels (runtime contexts) ----
        // These represent the default context each service runs in

        // Customer app starts orders
        //customerApp.addTag("ORDER_PLACED");

        // Restaurant service processes prepared food
        //restaurantSvc.addTag("FOOD_PREPARED");

        // Delivery service tracks active deliveries
        //deliverySvc.addTag("OUT_FOR_DELIVERY");

        // Payment service handles financial state
        //paymentSvc.addTag("PAYMENT_CAPTURED");

        // Support service may trigger refunds
        //supportSvc.addTag("REFUND_ISSUED");

        // Audit service always runs in audit context
        //auditSvc.addTag("AUDIT_LOG");

        /*
         * Resulting enforced flow:
         *
         * customerApp
         *   → ORDER_PLACED
         *
         * restaurantSvc
         *   ← ORDER_PLACED
         *   → FOOD_PREPARED
         *
         * deliverySvc
         *   ← ORDER_PLACED + FOOD_PREPARED
         *   → OUT_FOR_DELIVERY
         *
         * paymentSvc
         *   ← ORDER_PLACED + FOOD_PREPARED
         *   → PAYMENT_CAPTURED / REFUND_ISSUED
         *
         * supportSvc
         *   ← PAYMENT_CAPTURED
         *   → REFUND_ISSUED
         *
         * auditSvc
         *   ← all stages (via AUDIT_LOG)
         *
         * This prevents:
         *   - Delivery before preparation
         *   - Refunds without payment
         *   - Unauthorized state transitions
         */
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
        try
        {
            isValidClientId(clientId);
            return getClient(clientId);
        }
        catch (Exception e)
        {
            return new ClientDIFCPrivs(clientId);
        }
    }

    public ClientDIFCPrivs getClient(String clientId)
    {
        isValidClientId(clientId);
        ClientDIFCPrivs c = clientsById.get(clientId);
        if(c == null)
            throw new ClientNotFoundException("Client '" + clientId + "' does NOT exist");
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

     public Set<String> getTagsForClient(String clientId)
     {
         if(clientsById.get(clientId) == null)
             return Collections.emptySet();
         return Collections.unmodifiableSet(getOrCreateClient(clientId).getTags());
     }

     public Set<String> getPositiveCapacityTagsForClient(String clientId)
     {
         if(clientsById.get(clientId) == null)
             return Collections.emptySet();
         return Collections.unmodifiableSet(getOrCreateClient(clientId).getAddCapabilities());
     }

     public Set<String> getNegativeCapacityTagsForClient(String clientId)
     {
         if(clientsById.get(clientId) == null)
             return Collections.emptySet();
         return Collections.unmodifiableSet(getOrCreateClient(clientId).getRemoveCapabilities());
     }

     public Set<String> getOwnedTagsForClient(String clientId)
     {
         if(clientsById.get(clientId) == null)
             return Collections.emptySet();
         return Collections.unmodifiableSet(getOrCreateClient(clientId).getOwnedTags());
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
        ClientDIFCPrivs client = getClient(clientId);
        if(client.getAddCapabilities().contains(tagName))
        {
            client.addTag(tagName);
            return OK;
        }
        throw new UnAuthorizedClientException("Client  '" + clientId + "' is unauthorized to perform this operation.");
    }

    public int removeTag(String tagName, String clientId)
    {
        ClientDIFCPrivs client = getClient(clientId);

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
        ClientDIFCPrivs client = getClient(clientId);
        client.addCapability(tagName, cap);
        return OK;
    }

    public int removeClientPrivs(String clientId, String tagName, Capability cap)
    {
        if (clientId == null || tagName == null || cap == null)
            throw new NullInputException("clientId, tagName and cap must not be null");
        if(!tagsByName.containsKey(tagName))
            throw new TagNotFoundException("Tag '" + tagName + "' not found");
        ClientDIFCPrivs client = getClient(clientId);
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
        ClientDIFCPrivs client = getClient(clientId);

        if(!ownerClient.getOwnedTags().contains(tagName))
            throw new UnAuthorizedClientException("Client '" + clientId + "' is unauthorized to perform this operation as you do NOT own this tag.");

        ownerClient.removeOwnership(tagName);
        client.addOwnership(tagName);

        return OK;
    }

    public boolean canClientReceive(String receiverId, Set<String> messageTags)
    {
        try
        {
            for(String tagName : messageTags)
            {
                isValidTagName(tagName);
            }
        }
        catch (Exception e)
        {
            return true;
        }

        ClientDIFCPrivs receiver = getOrCreateClient(receiverId);
        return receiver.getTags().containsAll(messageTags);
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