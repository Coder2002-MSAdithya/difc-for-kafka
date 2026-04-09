package org.apache.kafka.image;

import org.apache.kafka.common.metadata.DifcTagCreatedRecord;
import org.apache.kafka.common.metadata.DifcTagDestroyedRecord;
import org.apache.kafka.common.metadata.DifcClientRegisteredRecord;
import org.apache.kafka.common.metadata.DifcClientLabelChangedRecord;
import org.apache.kafka.common.metadata.DifcClientPrivilegeChangedRecord;
import org.apache.kafka.common.metadata.DifcTagOwnershipTransferredRecord;
import org.apache.kafka.server.difc.Capability;
import org.apache.kafka.server.difc.ClientDIFCPrivs;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class DifcDelta
{
    private final DifcImage image;

    // Tracking Tag Changes (String -> OwnerId)
    private final Map<String, String> createdTags = new HashMap<>();
    private final Set<String> destroyedTags = new HashSet<>();

    // Tracking Client Changes
    private final Map<String, ClientDIFCPrivs> updatedClients = new HashMap<>();

    public DifcDelta(DifcImage image) { this.image = image; }

    public Map<String, String> createdTags() { return createdTags; }
    public Set<String> destroyedTags() { return destroyedTags; }
    public Map<String, ClientDIFCPrivs> updatedClients() { return updatedClients; }

    private ClientDIFCPrivs getAndCloneClient(String clientId)
    {
        if (updatedClients.containsKey(clientId)) { return updatedClients.get(clientId); }
        ClientDIFCPrivs existingClient = image.clients().get(clientId);
        if (existingClient != null)
        {
            ClientDIFCPrivs cloned = new ClientDIFCPrivs(existingClient);
            updatedClients.put(clientId, cloned);
            return cloned;
        }
        ClientDIFCPrivs newClient = new ClientDIFCPrivs(clientId);
        updatedClients.put(clientId, newClient);
        return newClient;
    }

    // =========================================================
    // REPLAY METHODS
    // =========================================================

    public void replay(DifcTagCreatedRecord record)
    {
        createdTags.put(record.tagName(), record.ownerClientId());
        destroyedTags.remove(record.tagName());

        ClientDIFCPrivs owner = getAndCloneClient(record.ownerClientId());
        owner.addOwnership(record.tagName());
    }

    public void replay(DifcTagDestroyedRecord record)
    {
        destroyedTags.add(record.tagName());
        createdTags.remove(record.tagName());

        for (ClientDIFCPrivs client : updatedClients.values()) {
            client.removeTag(record.tagName());
            client.removeCapability(record.tagName(), Capability.CAN_ADD);
            client.removeCapability(record.tagName(), Capability.CAN_REMOVE);
            client.removeOwnership(record.tagName());
        }
    }

    public void replay(DifcClientRegisteredRecord record)
    {
        updatedClients.put(record.clientId(), new ClientDIFCPrivs(record.clientId()));
    }

    public void replay(DifcClientLabelChangedRecord record)
    {
        ClientDIFCPrivs client = getAndCloneClient(record.clientId());
        if (record.action() == (byte) 1) {
            client.addTag(record.tagName());
        } else {
            client.removeTag(record.tagName());
        }
    }

    public void replay(DifcClientPrivilegeChangedRecord record)
    {
        ClientDIFCPrivs client = getAndCloneClient(record.clientId());
        Capability cap = record.capability() == (byte) 0 ? Capability.CAN_ADD : Capability.CAN_REMOVE;

        if (record.action() == (byte) 1) {
            client.addCapability(record.tagName(), cap);
        } else {
            client.removeCapability(record.tagName(), cap);
        }
    }

    public void replay(DifcTagOwnershipTransferredRecord record)
    {
        // 1. Update the master tag map so the snapshot saves the new owner
        createdTags.put(record.tagName(), record.toClientId());

        // 2. Strip ownership from old client
        ClientDIFCPrivs fromClient = getAndCloneClient(record.fromClientId());
        fromClient.removeOwnership(record.tagName());

        // 3. Grant ownership to new client
        ClientDIFCPrivs toClient = getAndCloneClient(record.toClientId());
        toClient.addOwnership(record.tagName());
    }

    // =========================================================
    // APPLY: Generate the new Immutable Image
    // =========================================================
    public DifcImage apply()
    {
        if (createdTags.isEmpty() && destroyedTags.isEmpty() && updatedClients.isEmpty())
        {
            return image;
        }

        Map<String, String> newTags = new HashMap<>(image.tags());
        for (String destroyed : destroyedTags) { newTags.remove(destroyed); }
        for (Map.Entry<String, String> entry : createdTags.entrySet())
        {
            newTags.put(entry.getKey(), entry.getValue());
        }

        Map<String, ClientDIFCPrivs> newClients = new HashMap<>(image.clients());

        // Strip destroyed tags from clients that weren't actively updated in this batch
        if (!destroyedTags.isEmpty())
        {
            for (Map.Entry<String, ClientDIFCPrivs> entry : newClients.entrySet())
            {
                if (!updatedClients.containsKey(entry.getKey()))
                {
                    ClientDIFCPrivs cloned = new ClientDIFCPrivs(entry.getValue());
                    for (String destroyedTag : destroyedTags)
                    {
                        cloned.removeTag(destroyedTag);
                        cloned.removeCapability(destroyedTag, Capability.CAN_ADD);
                        cloned.removeCapability(destroyedTag, Capability.CAN_REMOVE);
                        cloned.removeOwnership(destroyedTag); // Clean up ownership here too!
                    }
                    updatedClients.put(entry.getKey(), cloned);
                }
            }
        }

        for (Map.Entry<String, ClientDIFCPrivs> entry : updatedClients.entrySet())
        {
            newClients.put(entry.getKey(), entry.getValue());
        }

        return new DifcImage(newTags, newClients);
    }
}