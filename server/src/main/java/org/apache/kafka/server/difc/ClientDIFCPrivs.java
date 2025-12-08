package org.apache.kafka.server.difc;

import javax.print.DocFlavor;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class ClientDIFCPrivs
{
    public final String clientId;
    public final Set<String> tags; // current label of the DIFC client
    public final Set<String> canAdd;
    public final Set<String> canRemove;
    public final Set<String> owns;

    public ClientDIFCPrivs(String clientId)
    {
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.tags = Collections.synchronizedSet(new HashSet<>());
        this.canAdd = Collections.synchronizedSet(new HashSet<>());
        this.canRemove = Collections.synchronizedSet(new HashSet<>());
        this.owns = Collections.synchronizedSet(new HashSet<>());
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