package org.apache.kafka.controller;

import org.apache.kafka.common.message.*;
import org.apache.kafka.common.metadata.*;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.difc.*;

import java.util.Collections;
import java.util.List;

public class DifcControlManager
{
    // The controller's authoritative in-memory state
    private final TagRegistrar tagRegistrar;

    public DifcControlManager()
    {
        this.tagRegistrar = new TagRegistrar();
        this.tagRegistrar.initialize();
    }

    // =========================================================================
    // BOILERPLATE REDUCTION HELPERS
    // =========================================================================
    /** Helper to generate a failure response without writing to the Raft log. */
    private <T> ControllerResult<T> fail(T responseData)
    {
        return ControllerResult.of(Collections.emptyList(), responseData);
    }

    /** Helper to generate a success response and wrap the record for the Raft log. */
    private <T> ControllerResult<T> success(ApiMessage record, T responseData)
    {
        ApiMessageAndVersion recordAndVersion = new ApiMessageAndVersion(record, (short) 0);
        return ControllerResult.atomicOf(Collections.singletonList(recordAndVersion), responseData);
    }

    // =========================================================================
    // API HANDLERS
    // =========================================================================
    public ControllerResult<RegisterClientResponseData> registerDifcClient(String principalName)
    {
        RegisterClientResponseData response = new RegisterClientResponseData();

        if (!TagRegistrar.isValidClientId(principalName))
        {
            return fail(response.setErrorCode(Errors.DIFC_CLIENT_ID_INVALID.code())
                    .setErrorMessage("Invalid client ID format: " + principalName));
        }
        if (tagRegistrar.getClient(principalName) != null)
        {
            return fail(response.setErrorCode(Errors.DIFC_CLIENT_ALREADY_EXISTS.code())
                    .setErrorMessage("Client is already registered: " + principalName));
        }

        DifcClientRegisteredRecord record = new DifcClientRegisteredRecord().setClientId(principalName);
        return success(record, response.setErrorCode(Errors.NONE.code()));
    }

    public ControllerResult<CreateTagResponseData> createDifcTag(String tagName, String ownerPrincipalName)
    {
        CreateTagResponseData response = new CreateTagResponseData();

        if (!TagRegistrar.isValidTagName(tagName))
        {
            return fail(response.setErrorCode(Errors.DIFC_TAG_NAME_INVALID.code())
                    .setErrorMessage("Invalid tag name format: " + tagName));
        }
        if (!tagRegistrar.isTagNameAvailable(tagName))
        {
            return fail(response.setErrorCode(Errors.DIFC_TAG_NAME_ALREADY_EXISTS.code())
                    .setErrorMessage("Tag already exists: " + tagName));
        }
        if (tagRegistrar.getClient(ownerPrincipalName) == null)
        {
            return fail(response.setErrorCode(Errors.DIFC_CLIENT_NOT_FOUND.code())
                    .setErrorMessage("Owner client not found: " + ownerPrincipalName));
        }

        DifcTagCreatedRecord record = new DifcTagCreatedRecord()
                .setTagName(tagName)
                .setOwnerClientId(ownerPrincipalName);

        return success(record, response.setErrorCode(Errors.NONE.code()));
    }

    public ControllerResult<DestroyTagResponseData> destroyDifcTag(String tagName, String principalName)
    {
        DestroyTagResponseData response = new DestroyTagResponseData();

        if (!TagRegistrar.isValidTagName(tagName))
        {
            return fail(response.setErrorCode(Errors.DIFC_TAG_NAME_INVALID.code())
                    .setErrorMessage("Invalid tag name format."));
        }
        if (!tagRegistrar.hasTag(tagName))
        {
            return fail(response.setErrorCode(Errors.DIFC_TAG_NAME_NOT_FOUND.code())
                    .setErrorMessage("Tag not found."));
        }
        if (tagRegistrar.getClient(principalName) == null || !tagRegistrar.getOwnedTagsForClient(principalName).contains(tagName))
        {
            return fail(response.setErrorCode(Errors.DIFC_UNAUTHORIZED_CLIENT.code())
                    .setErrorMessage("Client is not authorized to destroy this tag."));
        }

        DifcTagDestroyedRecord record = new DifcTagDestroyedRecord()
                .setTagName(tagName)
                .setOwnerClientId(principalName);
        return success(record, response.setErrorCode(Errors.NONE.code()));
    }

    public ControllerResult<AddClientPrivsResponseData> addDifcClientPrivs(String owner, String target, String tag, Capability cap)
    {
        AddClientPrivsResponseData response = new AddClientPrivsResponseData();

        if (!TagRegistrar.isValidTagName(tag)) return fail(response.setErrorCode(Errors.DIFC_TAG_NAME_INVALID.code()).setErrorMessage("Invalid tag name format."));
        if (!tagRegistrar.hasTag(tag)) return fail(response.setErrorCode(Errors.DIFC_TAG_NAME_NOT_FOUND.code()).setErrorMessage("Tag not found."));
        if (tagRegistrar.getClient(owner) == null || !tagRegistrar.getOwnedTagsForClient(owner).contains(tag))
            return fail(response.setErrorCode(Errors.DIFC_UNAUTHORIZED_CLIENT.code()).setErrorMessage("Requesting client does not own the tag."));
        if (tagRegistrar.getClient(target) == null)
            return fail(response.setErrorCode(Errors.DIFC_CLIENT_NOT_FOUND.code()).setErrorMessage("Target client not found."));
        if (owner.equals(target))
            return fail(response.setErrorCode(Errors.DIFC_UNAUTHORIZED_CLIENT.code()).setErrorMessage("Cannot modify own privileges this way."));

        DifcClientPrivilegeChangedRecord record = new DifcClientPrivilegeChangedRecord()
                .setClientId(target).setTagName(tag).setAction((byte)1).setCapability((byte)cap.ordinal());
        return success(record, response.setErrorCode(Errors.NONE.code()));
    }

    public ControllerResult<RemoveClientPrivsResponseData> removeDifcClientPrivs(String owner, String target, String tag, Capability cap)
    {
        RemoveClientPrivsResponseData response = new RemoveClientPrivsResponseData();

        if (!TagRegistrar.isValidTagName(tag)) return fail(response.setErrorCode(Errors.DIFC_TAG_NAME_INVALID.code()).setErrorMessage("Invalid tag name format."));
        if (!tagRegistrar.hasTag(tag)) return fail(response.setErrorCode(Errors.DIFC_TAG_NAME_NOT_FOUND.code()).setErrorMessage("Tag not found."));
        if (tagRegistrar.getClient(owner) == null || !tagRegistrar.getOwnedTagsForClient(owner).contains(tag))
            return fail(response.setErrorCode(Errors.DIFC_UNAUTHORIZED_CLIENT.code()).setErrorMessage("Requesting client does not own the tag."));
        if (tagRegistrar.getClient(target) == null)
            return fail(response.setErrorCode(Errors.DIFC_CLIENT_NOT_FOUND.code()).setErrorMessage("Target client not found."));
        if (owner.equals(target))
            return fail(response.setErrorCode(Errors.DIFC_UNAUTHORIZED_CLIENT.code()).setErrorMessage("Cannot modify own privileges this way."));

        DifcClientPrivilegeChangedRecord record = new DifcClientPrivilegeChangedRecord()
                .setClientId(target).setTagName(tag).setAction((byte)0).setCapability((byte)cap.ordinal());
        return success(record, response.setErrorCode(Errors.NONE.code()));
    }

    public ControllerResult<AddTagResponseData> addDifcTag(String principalName, String tagName)
    {
        AddTagResponseData response = new AddTagResponseData();

        if (!TagRegistrar.isValidTagName(tagName)) return fail(response.setErrorCode(Errors.DIFC_TAG_NAME_INVALID.code()).setErrorMessage("Invalid tag name format."));
        if (!tagRegistrar.hasTag(tagName)) return fail(response.setErrorCode(Errors.DIFC_TAG_NAME_NOT_FOUND.code()).setErrorMessage("Tag not found."));
        if (tagRegistrar.getClient(principalName) == null) return fail(response.setErrorCode(Errors.DIFC_CLIENT_NOT_FOUND.code()).setErrorMessage("Client not found."));

        if (!tagRegistrar.getClient(principalName).canAdd(tagName) && !tagRegistrar.getOwnedTagsForClient(principalName).contains(tagName))
        {
            return fail(response.setErrorCode(Errors.DIFC_UNAUTHORIZED_CLIENT.code()).setErrorMessage("Client lacks CAN_ADD capability or ownership."));
        }

        DifcClientLabelChangedRecord record = new DifcClientLabelChangedRecord()
                .setClientId(principalName).setTagName(tagName).setAction((byte)1);
        return success(record, response.setErrorCode(Errors.NONE.code()));
    }

    public ControllerResult<RemoveTagResponseData> removeDifcTag(String principalName, String tagName)
    {
        RemoveTagResponseData response = new RemoveTagResponseData();

        if (!TagRegistrar.isValidTagName(tagName)) return fail(response.setErrorCode(Errors.DIFC_TAG_NAME_INVALID.code()).setErrorMessage("Invalid tag name format."));
        if (!tagRegistrar.hasTag(tagName)) return fail(response.setErrorCode(Errors.DIFC_TAG_NAME_NOT_FOUND.code()).setErrorMessage("Tag not found."));
        if (tagRegistrar.getClient(principalName) == null) return fail(response.setErrorCode(Errors.DIFC_CLIENT_NOT_FOUND.code()).setErrorMessage("Client not found."));

        if (!tagRegistrar.getClient(principalName).canRemove(tagName) && !tagRegistrar.getOwnedTagsForClient(principalName).contains(tagName)) {
            return fail(response.setErrorCode(Errors.DIFC_UNAUTHORIZED_CLIENT.code()).setErrorMessage("Client lacks CAN_REMOVE capability or ownership."));
        }

        DifcClientLabelChangedRecord record = new DifcClientLabelChangedRecord()
                .setClientId(principalName).setTagName(tagName).setAction((byte)0);
        return success(record, response.setErrorCode(Errors.NONE.code()));
    }

    public ControllerResult<GrantOwnerPrivilegesResponseData> grantOwnerPrivileges(String owner, String target, String tag)
    {
        GrantOwnerPrivilegesResponseData response = new GrantOwnerPrivilegesResponseData();

        if (!TagRegistrar.isValidTagName(tag)) return fail(response.setErrorCode(Errors.DIFC_TAG_NAME_INVALID.code()).setErrorMessage("Invalid tag name format."));
        if (!tagRegistrar.hasTag(tag)) return fail(response.setErrorCode(Errors.DIFC_TAG_NAME_NOT_FOUND.code()).setErrorMessage("Tag not found."));
        if (tagRegistrar.getClient(owner) == null || !tagRegistrar.getOwnedTagsForClient(owner).contains(tag))
            return fail(response.setErrorCode(Errors.DIFC_UNAUTHORIZED_CLIENT.code()).setErrorMessage("Requesting client does not own the tag."));
        if (tagRegistrar.getClient(target) == null)
            return fail(response.setErrorCode(Errors.DIFC_CLIENT_NOT_FOUND.code()).setErrorMessage("Target client not found."));

        DifcTagOwnershipTransferredRecord record = new DifcTagOwnershipTransferredRecord().setFromClientId(owner).setToClientId(target).setTagName(tag);
        return success(record, response.setErrorCode(Errors.NONE.code()));
    }

    public ControllerResult<GrantCapResponseData> enqueueCapabilityRequest(
            String tagName, String capabilityString, String requesterPrincipal, byte[] attestedPolicyBytes)
    {
        GrantCapResponseData response = new GrantCapResponseData();

        if (!tagRegistrar.hasTag(tagName)) {
            return fail(response.setErrorCode(Errors.DIFC_TAG_NAME_NOT_FOUND.code())
                    .setErrorMessage("Tag not found."));
        }

        Capability cap;
        try {
            cap = Capability.valueOf(capabilityString.toUpperCase());
        } catch (IllegalArgumentException e) {
            return fail(response.setErrorCode(Errors.DIFC_UNSUPPORTED_CAPABILITY.code())
                    .setErrorMessage("Invalid capability string."));
        }

        ClientDIFCPrivs owner = tagRegistrar.getOwner(tagName);
        if (owner == null) {
            return fail(response.setErrorCode(Errors.DIFC_CLIENT_NOT_FOUND.code())
                    .setErrorMessage("Tag owner not found."));
        }

        if (owner.getClientId().equals(requesterPrincipal)) {
            return fail(response.setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("You cannot request capabilities for a tag you already own."));
        }

        tagRegistrar.enqueueCapabilityRequest(owner.getClientId(), tagName, cap, requesterPrincipal, attestedPolicyBytes);

        DifcGrantCapRequestRecord record = new DifcGrantCapRequestRecord()
                .setRequesterPrincipal(requesterPrincipal)
                .setTagName(tagName)
                .setCapability(capabilityString)
                .setAttestedPolicy(attestedPolicyBytes);
        return success(record, response.setErrorCode(Errors.NONE.code()).setErrorMessage("Success"));
    }

    public ControllerResult<PollPrivsReqResponseData> pollPendingRequests(String clientId)
    {
        PollPrivsReqResponseData response = new PollPrivsReqResponseData();

        // Pop the front element from the queue
        CapabilityRequest req = tagRegistrar.pollCapabilityRequest(clientId);

        if (req != null) {
            response.setTagName(req.getTagName());
            response.setCapability((byte) req.getCapability().ordinal());
            response.setRequesterClientId(req.getFromClientId());
            response.setAttestedPolicy(req.attestedPolicyBytes());
        } else {
            // Queue is empty or client not found
            response.setTagName("");
            response.setCapability((byte) -1); // -1 maps to "none" as per your JSON schema
            response.setRequesterClientId("");
        }

        return ControllerResult.of(Collections.emptyList(), response);
    }

    // =========================================================================
    // REPLAY METHODS (Memory Mutations)
    // =========================================================================

    void replay(DifcTagCreatedRecord record)
    {
        tagRegistrar.createTag(record.tagName(), record.ownerClientId());
    }

    void replay(DifcTagDestroyedRecord record)
    {
        tagRegistrar.destroyTag(record.tagName(), record.ownerClientId());
    }

    void replay(DifcClientRegisteredRecord record)
    {
        tagRegistrar.registerClient(record.clientId());
    }

    void replay(DifcClientLabelChangedRecord record)
    {
        if (record.action() == (byte) 1)
        {
            tagRegistrar.addTag(record.tagName(), record.clientId());
        }
        else
        {
            tagRegistrar.removeTag(record.tagName(), record.clientId());
        }
    }

    void replay(DifcClientPrivilegeChangedRecord record)
    {
        Capability cap = record.capability() == (byte) 0 ? Capability.CAN_ADD : Capability.CAN_REMOVE;

        if (record.action() == (byte) 1)
        {
            tagRegistrar.addClientPrivs(record.clientId(), record.tagName(), cap);
        }
        else
        {
            tagRegistrar.removeClientPrivs(record.clientId(), record.tagName(), cap);
        }
    }

    void replay(DifcTagOwnershipTransferredRecord record)
    {
        tagRegistrar.grantOwnerPrivileges(record.fromClientId(), record.toClientId(), record.tagName());
    }

    void replay(DifcGrantCapRequestRecord record)
    {
        Capability cap;
        try {
            cap = Capability.valueOf(record.capability().toUpperCase());
        } catch (IllegalArgumentException e) {
            return;
        }
        ClientDIFCPrivs owner = tagRegistrar.getOwner(record.tagName());
        if (owner == null) {
            return;
        }
        tagRegistrar.enqueueCapabilityRequest(
                owner.getClientId(), record.tagName(), cap, record.requesterPrincipal(), record.attestedPolicy());
    }
}