package org.apache.kafka.streams.difc;

import org.apache.kafka.clients.Capability;
import org.apache.kafka.common.message.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.internals.DifcStreamsRuntime;

public final class StreamsDIFC
{
    private final DifcSyncFacade sync;

    private StreamsDIFC(DifcSyncFacade sync)
    {
        this.sync = sync;
    }

    /**
     * Entry point for Streams users.
     * Provides access to synchronous DIFC operations.
     */
    public static StreamsDIFC from(KafkaStreams streams)
    {
        // streams parameter is for scoping/clarity,
        // runtime is already bound via client supplier
        DifcSyncFacade facade = DifcStreamsRuntime.difc();
        return new StreamsDIFC(facade);
    }

    // ---- expose ONLY synchronous DIFC methods ----

    public RegisterClientResponseData registerClient()
    {
        return sync.registerClient();
    }

    public CreateTagResponseData createTag(String tag)
    {
        return sync.createTag(tag);
    }

    public DestroyTagResponseData destroyTag(String tag)
    {
        return sync.destroyTag(tag);
    }

    public AddTagResponseData addTag(String tag)
    {
        return sync.addTag(tag);
    }

    public RemoveTagResponseData removeTag(String tag)
    {
        return sync.removeTag(tag);
    }

    public AddClientPrivsResponseData addClientPrivs(
            String targetClientId,
            String tag,
            Capability cap)
    {
        return sync.addClientPrivs(targetClientId, tag, cap);
    }

    public RemoveClientPrivsResponseData removeClientPrivs(
            String targetClientId,
            String tag,
            Capability cap)
    {
        return sync.removeClientPrivs(targetClientId, tag, cap);
    }

    public GrantOwnerPrivilegesResponseData grantOwner(
            String targetClientId,
            String tag)
    {
        return sync.grantOwner(targetClientId, tag);
    }
}

