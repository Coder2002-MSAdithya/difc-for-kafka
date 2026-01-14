package org.apache.kafka.streams.difc;

import org.apache.kafka.clients.Capability;
import org.apache.kafka.clients.producer.DifcKafkaProducer;
import org.apache.kafka.common.message.*;

public final class DifcSyncFacade {

    private final DifcKafkaProducer<?, ?> producer;

    public DifcSyncFacade(DifcKafkaProducer<?, ?> producer) {
        this.producer = producer;
    }

    public RegisterClientResponseData registerClient() {
        // client.id already == application.id
        return producer.sendRegisterClientRequest(
                producer.getClientId()
        );
    }

    public CreateTagResponseData createTag(String tagName) {
        return producer.sendCreateTagRequest(tagName);
    }

    public DestroyTagResponseData destroyTag(String tagName) {
        return producer.sendDestroyTagRequest(tagName);
    }

    public AddTagResponseData addTag(String tagName) {
        return producer.sendAddTagRequest(tagName);
    }

    public RemoveTagResponseData removeTag(String tagName) {
        return producer.sendRemoveTagRequest(tagName);
    }

    public AddClientPrivsResponseData addClientPrivs(
            String targetClientId,
            String tagName,
            Capability capability) {

        return producer.sendAddClientPrivsRequest(
                targetClientId, tagName, capability);
    }

    public RemoveClientPrivsResponseData removeClientPrivs(
            String targetClientId,
            String tagName,
            Capability capability) {

        return producer.sendRemoveClientPrivsRequest(
                targetClientId, tagName, capability);
    }

    public GrantOwnerPrivilegesResponseData grantOwner(
            String targetClientId,
            String tagName) {

        return producer.sendGrantOwnerPrivilegesRequest(
                targetClientId, tagName);
    }
}
