package org.apache.kafka.server.difc;

import org.apache.kafka.server.difc.exceptions.*;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TagRegistrarTest {

    void testInitialization() {
        TagRegistrar registrar = new TagRegistrar();
        registrar.initialize();

        // ---- 1. Verify tag count ----
        // ORDER_PLACED, FOOD_PREPARED, OUT_FOR_DELIVERY,
        // PAYMENT_CAPTURED, REFUND_ISSUED, AUDIT_LOG
        assertEquals(6, registrar.getTagCount());

        // ---- 2. Verify tags exist ----
        assertTrue(registrar.hasTag("ORDER_PLACED"));
        assertTrue(registrar.hasTag("FOOD_PREPARED"));
        assertTrue(registrar.hasTag("OUT_FOR_DELIVERY"));
        assertTrue(registrar.hasTag("PAYMENT_CAPTURED"));
        assertTrue(registrar.hasTag("REFUND_ISSUED"));
        assertTrue(registrar.hasTag("AUDIT_LOG"));

        // ---- 3. Verify client count ----
        assertEquals(6, registrar.getClientCount());

        // ---- 4. Validate each client ----

        // ---- customerApp ----
        ClientDIFCPrivs customerApp = registrar.getClient("customerApp");
        assertTrue(customerApp.getOwnedTags().contains("ORDER_PLACED"));
        assertTrue(customerApp.getAddCapabilities().contains("ORDER_PLACED"));
        assertTrue(customerApp.getTags().contains("ORDER_PLACED"));

        // ---- restaurantSvc ----
        ClientDIFCPrivs restaurantSvc = registrar.getClient("restaurantSvc");
        assertTrue(restaurantSvc.getOwnedTags().contains("FOOD_PREPARED"));
        assertTrue(restaurantSvc.getAddCapabilities().contains("FOOD_PREPARED"));
        assertTrue(restaurantSvc.getRemoveCapabilities().contains("FOOD_PREPARED"));
        assertTrue(restaurantSvc.getTags().contains("FOOD_PREPARED"));

        // ---- deliverySvc ----
        ClientDIFCPrivs deliverySvc = registrar.getClient("deliverySvc");
        assertTrue(deliverySvc.getOwnedTags().contains("OUT_FOR_DELIVERY"));
        assertTrue(deliverySvc.getAddCapabilities().contains("OUT_FOR_DELIVERY"));
        assertTrue(deliverySvc.getRemoveCapabilities().contains("OUT_FOR_DELIVERY"));
        assertTrue(deliverySvc.getTags().contains("OUT_FOR_DELIVERY"));

        // ---- paymentSvc ----
        ClientDIFCPrivs paymentSvc = registrar.getClient("paymentSvc");
        assertTrue(paymentSvc.getOwnedTags().contains("PAYMENT_CAPTURED"));
        assertTrue(paymentSvc.getOwnedTags().contains("REFUND_ISSUED"));

        assertTrue(paymentSvc.getAddCapabilities().contains("PAYMENT_CAPTURED"));
        assertTrue(paymentSvc.getRemoveCapabilities().contains("PAYMENT_CAPTURED"));
        assertTrue(paymentSvc.getAddCapabilities().contains("REFUND_ISSUED"));

        assertTrue(paymentSvc.getTags().contains("PAYMENT_CAPTURED"));
        // REFUND_ISSUED is NOT in initial context for paymentSvc
        assertFalse(paymentSvc.getTags().contains("REFUND_ISSUED"));

        // ---- supportSvc ----
        ClientDIFCPrivs supportSvc = registrar.getClient("supportSvc");
        assertFalse(supportSvc.getOwnedTags().contains("REFUND_ISSUED"));
        assertTrue(supportSvc.getAddCapabilities().contains("REFUND_ISSUED"));
        assertTrue(supportSvc.getTags().contains("REFUND_ISSUED"));

        // ---- auditSvc ----
        ClientDIFCPrivs auditSvc = registrar.getClient("auditSvc");
        assertTrue(auditSvc.getOwnedTags().contains("AUDIT_LOG"));
        assertTrue(auditSvc.getAddCapabilities().contains("AUDIT_LOG"));
        assertTrue(auditSvc.getTags().contains("AUDIT_LOG"));
    }

    @Test
    void testCreateTagSuccess()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.registerClient("clientX");

        int tagId = registrar.createTag("newTag", "clientX");
        assertTrue(tagId > 0);
        assertTrue(registrar.hasTag("newTag"));

        ClientDIFCPrivs client = registrar.getClient("clientX");
        assertTrue(client.getOwnedTags().contains("newTag"));
    }

    @Test
    void testTooLongTagName()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.registerClient("clientX");
        assertThrows(InvalidTagNameException.class, () -> registrar.createTag("newTagToooooooooooooooooooLoooooNG", "clientX"));
    }

    @Test
    void testCreateTagDuplicate()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.registerClient("clientX");
        registrar.createTag("dupTag", "clientX");

        registrar.registerClient("clientY");
        assertThrows(DuplicateTagException.class,
                () -> registrar.createTag("dupTag", "clientY"));
    }

    @Test
    void testCreateTagInvalidName()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.registerClient("clientX");

        assertThrows(InvalidTagNameException.class,
                () -> registrar.createTag("invalid@tag", "clientX"));
        assertThrows(InvalidTagNameException.class, () -> registrar.hasTag("invalid@tag"));
    }

    @Test
    void testDestroyTag()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.registerClient("clientX");
        registrar.createTag("toDestroy", "clientX");

        int result = registrar.destroyTag("toDestroy", "clientX");
        assertEquals(0, result);
        assertFalse(registrar.hasTag("toDestroy"));

        ClientDIFCPrivs client = registrar.getClient("clientX");
        assertFalse(client.getOwnedTags().contains("toDestroy"));
    }

    @Test
    void testDestroyNonExistentTag()
    {
        TagRegistrar registrar = new TagRegistrar();
        assertThrows(TagNotFoundException.class,
                () -> registrar.destroyTag("nonexistent", "clientX"));
    }

    @Test
    void testGetTag()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.registerClient("clientX");

        int tagId = registrar.createTag("getTag", "clientX");
        int retrievedId = registrar.getTag("getTag");
        assertEquals(tagId, retrievedId);
        assertEquals(-1, registrar.getTag("nonexistent"));
    }

    @Test
    void testAddClientPrivs()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.registerClient("clientX");
        registrar.createTag("privTag", "clientX");

        int result = registrar.addClientPrivs("clientY", "privTag", Capability.CAN_ADD);
        assertEquals(0, result);

        ClientDIFCPrivs clientY = registrar.getClient("clientY");
        assertTrue(clientY.getAddCapabilities().contains("privTag"));
    }

    @Test
    void testAddClientPrivsNonExistentTag()
    {
        TagRegistrar registrar = new TagRegistrar();

        assertThrows(TagNotFoundException.class,
                () -> registrar.addClientPrivs("clientY", "nonexistent", Capability.CAN_ADD));
    }

    @Test
    void testRemoveClientPrivs()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.registerClient("clientX");
        registrar.createTag("privTag", "clientX");

        registrar.addClientPrivs("clientY", "privTag", Capability.CAN_ADD);

        int result = registrar.removeClientPrivs("clientY", "privTag", Capability.CAN_ADD);
        assertEquals(0, result);

        ClientDIFCPrivs clientY = registrar.getClient("clientY");
        assertFalse(clientY.getAddCapabilities().contains("privTag"));
    }

    @Test
    void testRemoveClientPrivsNonExistentTag()
    {
        TagRegistrar registrar = new TagRegistrar();

        assertThrows(TagNotFoundException.class,
                () -> registrar.removeClientPrivs("clientY", "nonexistent", Capability.CAN_ADD));
    }

    @Test
    public void testCanClientReceiveUnionSubset()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.registerClient("client2");

        // Setup: Add tags to clients
        registrar.createTag("tagX", "client2");
        registrar.createTag("tagY", "client2");
        registrar.addClientPrivs("client2", "tagX", Capability.CAN_ADD); // Ensure client2 is created

        ClientDIFCPrivs receiver = registrar.getClient("client2");

        receiver.addTag("tagX");
        receiver.addTag("tagY");

        Set<String> messageTags = new HashSet<>(Arrays.asList("tagY"));
        assertTrue(registrar.canClientReceive("client2", messageTags));
    }

    @Test
    public void testCanClientReceiveUnionNotSubset()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.registerClient("client2");

        // Setup: Add tags to clients
        registrar.createTag("tagX", "client2");
        registrar.createTag("tagY", "client2");
        registrar.addClientPrivs("client2", "tagX", Capability.CAN_ADD); // Ensure client2 is created

        ClientDIFCPrivs receiver = registrar.getClient("client2");
        receiver.addTag("tagX");

        Set<String> messageTags = new HashSet<>(Arrays.asList("tagY"));
        assertFalse(registrar.canClientReceive("client2", messageTags));
    }

    @Test
    void testCreateTagNonExistingOwner()
    {
        TagRegistrar registrar = new TagRegistrar();

        assertThrows(ClientNotFoundException.class,
                () -> registrar.createTag("newTag", "unknown"));
        assertFalse(registrar.hasTag("newTag"));
    }

    @Test
    void testRegisterClientSuccess()
    {
        TagRegistrar registrar = new TagRegistrar();
        ClientDIFCPrivs newClient = registrar.registerClient("newClient");
        assertEquals("newClient", newClient.getClientId());
        assertTrue(newClient.getTags().isEmpty());
        assertTrue(newClient.getAddCapabilities().isEmpty());
        assertTrue(newClient.getRemoveCapabilities().isEmpty());
        assertTrue(newClient.getOwnedTags().isEmpty());
        assertNotNull(registrar.getClient("newClient"));
    }

    @Test
    void testRegisterClientDuplicate()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.registerClient("dupClient");

        assertThrows(ClientExistsException.class,
                () -> registrar.registerClient("dupClient"));
    }

    @Test
    public void testCanClientReceiveNonExistingClient()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.initialize();

        Set<String> messageTags = new HashSet<>();
        assertThrows(ClientNotFoundException.class, () -> registrar.canClientReceive("nonexistent", messageTags));
    }

    @Test
    public void testCanClientReceiveWithUnknownMessageTag()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.initialize();

        Set<String> messageTags = new HashSet<>(Arrays.asList("unknownTag"));
        // client1 tags: tagA, tagB; union: tagA, tagB, unknownTag; not subset of itself unless it has unknownTag
        assertFalse(registrar.canClientReceive("deliverySvc", messageTags));
    }
}
