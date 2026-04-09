package org.apache.kafka.image;

import org.apache.kafka.common.metadata.DifcTagCreatedRecord;
import org.apache.kafka.common.metadata.DifcClientRegisteredRecord;
import org.apache.kafka.common.metadata.DifcClientLabelChangedRecord;
import org.apache.kafka.common.metadata.DifcClientPrivilegeChangedRecord;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.server.difc.ClientDIFCPrivs;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public final class DifcImage
{
    public static final DifcImage EMPTY = new DifcImage(Collections.emptyMap(), Collections.emptyMap());

    // State 1: The Tags (tagName -> ownerClientId)
    private final Map<String, String> tags;

    // State 2: The Clients (clientId -> Privileges)
    private final Map<String, ClientDIFCPrivs> clients;

    public DifcImage(Map<String, String> tags, Map<String, ClientDIFCPrivs> clients)
    {
        this.tags = Collections.unmodifiableMap(new HashMap<>(tags));
        this.clients = Collections.unmodifiableMap(new HashMap<>(clients));
    }

    public boolean isEmpty() { return tags.isEmpty() && clients.isEmpty(); }
    public Map<String, String> tags() { return tags; }
    public Map<String, ClientDIFCPrivs> clients() { return clients; }

    /**
     * Converts memory back into Raft records to save to disk during a Snapshot.
     */
    public void write(ImageWriter writer, ImageWriterOptions options)
    {
        // 1. Save all Tags (This implicitly saves the current Ownership!)
        for (Entry<String, String> entry : tags.entrySet())
        {
            writer.write(0, new DifcTagCreatedRecord()
                    .setTagName(entry.getKey())
                    .setOwnerClientId(entry.getValue())
            );
        }

        // 2. Save all Clients and their capabilities/labels
        for (Entry<String, ClientDIFCPrivs> entry : clients.entrySet())
        {
            String clientId = entry.getKey();
            ClientDIFCPrivs privs = entry.getValue();

            // A. Base Registration
            writer.write(0, new DifcClientRegisteredRecord()
                    .setClientId(clientId)
            );

            // B. Save Active Labels
            for (String tag : privs.getTags()) {
                writer.write(0, new DifcClientLabelChangedRecord()
                        .setClientId(clientId)
                        .setTagName(tag)
                        .setAction((byte) 1) // 1 to Add
                );
            }

            // C. Save CAN_ADD Privileges (Capability = 0)
            for (String tag : privs.getAddCapabilities()) {
                writer.write(0, new DifcClientPrivilegeChangedRecord()
                        .setClientId(clientId)
                        .setTagName(tag)
                        .setCapability((byte) 0)
                        .setAction((byte) 1) // 1 to Grant
                );
            }

            // D. Save CAN_REMOVE Privileges (Capability = 1)
            for (String tag : privs.getRemoveCapabilities()) {
                writer.write(0, new DifcClientPrivilegeChangedRecord()
                        .setClientId(clientId)
                        .setTagName(tag)
                        .setCapability((byte) 1)
                        .setAction((byte) 1) // 1 to Grant
                );
            }
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof DifcImage)) return false;
        DifcImage other = (DifcImage) o;
        return tags.equals(other.tags) && clients.equals(other.clients);
    }

    @Override
    public int hashCode() { return tags.hashCode() ^ clients.hashCode(); }

    @Override
    public String toString() { return "DifcImage(tags=" + tags.size() + ", clients=" + clients.size() + ")"; }
}