package org.apache.kafka.server.difc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TagRegistrarTest {
    @Test
    void testInitialization() {
        TagRegistrar registrar = new TagRegistrar();
        registrar.initialize();

        assertEquals(4, registrar.getTagCount()); // tagA, tagB, tagC, tagD
        assertTrue(registrar.hasTag("tagA"));
        assertEquals(3, registrar.getClientCount()); // client1,2,3

        ClientDIFCPrivs c1 = registrar.getClientPrivs("client1");
        assertTrue(c1.owns.contains("tagA"));
        assertTrue(c1.owns.contains("tagB"));
        assertTrue(c1.canAdd.contains("tagC"));
        assertTrue(c1.canRemove.contains("tagD"));
        assertTrue(c1.tags.contains("tagA"));
        assertTrue(c1.tags.contains("tagB"));

        System.out.println("testInitialization passed");
    }

    @Test
    void testCreateTagSuccess() {
        TagRegistrar registrar = new TagRegistrar();
        int tagId = registrar.createTag("newTag", "clientX");
        assertTrue(tagId > 0);
        assertTrue(registrar.hasTag("newTag"));
        ClientDIFCPrivs client = registrar.getClientPrivs("clientX");
        assertTrue(client.owns.contains("newTag"));
        System.out.println("testCreateTagSuccess passed");
    }

    @Test
    void testCreateTagDuplicate() {
        TagRegistrar registrar = new TagRegistrar();
        registrar.createTag("dupTag", "clientX");
        int result = registrar.createTag("dupTag", "clientY");
        assertEquals(-1, result);
        System.out.println("testCreateTagDuplicate passed");
    }

    @Test
    void testCreateTagInvalidName() {
        TagRegistrar registrar = new TagRegistrar();
        int result = registrar.createTag("invalid@tag", "clientX");
        assertEquals(-1, result);
        assertFalse(registrar.hasTag("invalid@tag"));
        System.out.println("testCreateTagInvalidName passed");
    }

    @Test
    void testDestroyTag() {
        TagRegistrar registrar = new TagRegistrar();
        registrar.createTag("toDestroy", "clientX");
        int result = registrar.destroyTag("toDestroy");
        assertEquals(0, result);
        assertFalse(registrar.hasTag("toDestroy"));
        ClientDIFCPrivs client = registrar.getClientPrivs("clientX");
        assertFalse(client.owns.contains("toDestroy"));
        System.out.println("testDestroyTag passed");
    }

    @Test
    void testDestroyNonExistentTag() {
        TagRegistrar registrar = new TagRegistrar();
        int result = registrar.destroyTag("nonexistent");
        assertEquals(-1, result);
        System.out.println("testDestroyNonExistentTag passed");
    }

    @Test
    void testGetTag() {
        TagRegistrar registrar = new TagRegistrar();
        int tagId = registrar.createTag("getTag", "clientX");
        int retrievedId = registrar.getTag("getTag");
        assertEquals(tagId, retrievedId);
        assertEquals(-1, registrar.getTag("nonexistent"));
        System.out.println("testGetTag passed");
    }

    @Test
    void testAddClientPrivs() {
        TagRegistrar registrar = new TagRegistrar();
        registrar.createTag("privTag", "clientX");
        int result = registrar.addClientPrivs("clientY", "privTag", Capability.CAN_ADD);
        assertEquals(0, result);
        ClientDIFCPrivs clientY = registrar.getClientPrivs("clientY");
        assertTrue(clientY.canAdd.contains("privTag"));
        System.out.println("testAddClientPrivs passed");
    }

    @Test
    void testAddClientPrivsNonExistentTag() {
        TagRegistrar registrar = new TagRegistrar();
        int result = registrar.addClientPrivs("clientY", "nonexistent", Capability.CAN_ADD);
        assertEquals(-1, result);
        System.out.println("testAddClientPrivsNonExistentTag passed");
    }

    @Test
    void testRemoveClientPrivs() {
        TagRegistrar registrar = new TagRegistrar();
        registrar.createTag("privTag", "clientX");
        registrar.addClientPrivs("clientY", "privTag", Capability.CAN_ADD);
        int result = registrar.removeClientPrivs("clientY", "privTag", Capability.CAN_ADD);
        assertEquals(0, result);
        ClientDIFCPrivs clientY = registrar.getClientPrivs("clientY");
        assertFalse(clientY.canAdd.contains("privTag"));
        System.out.println("testRemoveClientPrivs passed");
    }

    @Test
    void testRemoveClientPrivsNonExistentTag() {
        TagRegistrar registrar = new TagRegistrar();
        int result = registrar.removeClientPrivs("clientY", "nonexistent", Capability.CAN_ADD);
        assertEquals(-1, result);
        System.out.println("testRemoveClientPrivsNonExistentTag passed");
    }
}
