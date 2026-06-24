package org.apache.kafka.streams.difc;

import org.apache.kafka.clients.Capability;
import org.apache.kafka.clients.producer.DifcKafkaProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.message.*;

public final class DifcSyncFacade {

    private final KafkaProducer<?, ?> producer;

    public DifcSyncFacade(KafkaProducer<?, ?> producer) {
        this.producer = producer;
    }

    public RegisterClientResponseData registerClient() {return producer.registerClient();}

    public CreateTagResponseData createTag(String tagName) {
        return producer.createTag(tagName);
    }

    public DestroyTagResponseData destroyTag(String tagName) {
        return producer.destroyTag(tagName);
    }

    public AddTagResponseData addTag(String tagName) {
        return producer.addTag(tagName);
    }

    public RemoveTagResponseData removeTag(String tagName) {
        return producer.removeTag(tagName);
    }

    public AddClientPrivsResponseData addClientPrivs(
            String targetClientId,
            String tagName,
            Capability capability) {

        return producer.addClientPrivs(
                targetClientId, tagName, capability);
    }

    public RemoveClientPrivsResponseData removeClientPrivs(
            String targetClientId,
            String tagName,
            Capability capability) {

        return producer.removeClientPrivs(
                targetClientId, tagName, capability);
    }

    public GrantOwnerPrivilegesResponseData grantOwner(
            String targetClientId,
            String tagName) {

        return producer.grantOwnerPrivileges(
                targetClientId, tagName);
    }

    public GetPosCapsResponseData getAddCapabilities() {
        return producer.getAddCapabilities();
    }

    public GetNegCapsResponseData getRemoveCapabilities() {
        return producer.getRemoveCapabilities();
    }

    public GrantCapResponseData requestGrantCap(final String tagName, final Capability capability) {
        return producer.requestGrantCap(tagName, capability);
    }

    public GrantCapResponseData requestGrantCap(final String tagName, final Capability capability, final byte[] attestedPolicy) {
        return producer.requestGrantCap(tagName, capability, attestedPolicy);
    }

    public GrantCapResponseData requestAddCapabilityForTag(final String tagName) {
        return producer.requestAddCapabilityForTag(tagName);
    }

    public GrantCapResponseData requestAddCapabilityForTag(final String tagName, final byte[] attestedPolicy) {
        return producer.requestAddCapabilityForTag(tagName, attestedPolicy);
    }

    public GrantCapResponseData requestRemoveCapabilityForTag(final String tagName) {
        return producer.requestRemoveCapabilityForTag(tagName);
    }

    public GrantCapResponseData requestRemoveCapabilityForTag(final String tagName, final byte[] attestedPolicy) {
        return producer.requestRemoveCapabilityForTag(tagName, attestedPolicy);
    }
}
