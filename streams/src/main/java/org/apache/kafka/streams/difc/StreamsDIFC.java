package org.apache.kafka.streams.difc;

import org.apache.kafka.clients.Capability;
import org.apache.kafka.common.message.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.internals.DifcStreamsRuntime;

import java.time.Duration;
import java.util.List;

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

    public GetPosCapsResponseData getAddCapabilities()
    {
        return sync.getAddCapabilities();
    }

    public GetNegCapsResponseData getRemoveCapabilities()
    {
        return sync.getRemoveCapabilities();
    }

    public GrantCapResponseData requestGrantCap(final String tagName, final Capability capability)
    {
        return sync.requestGrantCap(tagName, capability);
    }

    public GrantCapResponseData requestGrantCap(final String tagName, final Capability capability, final byte[] attestedPolicy)
    {
        return sync.requestGrantCap(tagName, capability, attestedPolicy);
    }

    public GrantCapResponseData requestAddCapabilityForTag(final String tagName)
    {
        return sync.requestAddCapabilityForTag(tagName);
    }

    public GrantCapResponseData requestAddCapabilityForTag(final String tagName, final byte[] attestedPolicy)
    {
        return sync.requestAddCapabilityForTag(tagName, attestedPolicy);
    }

    public GrantCapResponseData requestRemoveCapabilityForTag(final String tagName)
    {
        return sync.requestRemoveCapabilityForTag(tagName);
    }

    public GrantCapResponseData requestRemoveCapabilityForTag(final String tagName, final byte[] attestedPolicy)
    {
        return sync.requestRemoveCapabilityForTag(tagName, attestedPolicy);
    }

    public boolean waitForAddCapability(
            final String capability,
            final Duration timeout,
            final Duration pollInterval)
    {
        return waitForCapability(capability, timeout, pollInterval, true);
    }

    public boolean waitForRemoveCapability(
            final String capability,
            final Duration timeout,
            final Duration pollInterval)
    {
        return waitForCapability(capability, timeout, pollInterval, false);
    }

    private boolean waitForCapability(
            final String capability,
            final Duration timeout,
            final Duration pollInterval,
            final boolean positive)
    {
        final long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
        final long pollMs = Math.max(1L, pollInterval.toMillis());

        while (System.currentTimeMillis() <= deadlineMs) {
            final List<String> capabilities = positive
                    ? getAddCapabilities().positiveCapabilities()
                    : getRemoveCapabilities().negativeCapabilities();

            if (capabilities.contains(capability)) {
                return true;
            }

            try {
                Thread.sleep(pollMs);
            }
            catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }
}

