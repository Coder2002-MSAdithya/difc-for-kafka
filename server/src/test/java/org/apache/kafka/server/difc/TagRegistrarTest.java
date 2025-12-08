package org.apache.kafka.server.difc;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class TagRegistrarTest {

    @Test
    public void testInitialization() {
        TagRegistrar registrar = new TagRegistrar();
        registrar.initialize();

        assertEquals(4, registrar.tagsByName.size()); // tagA, tagB, tagC, tagD
        assertTrue(registrar.tagsByName.containsKey("tagA"));
        assertEquals(3, registrar.clientsById.size()); // client1,2,3

        ClientDIFCPrivs c1 = registrar.clientsById.get("client1");
        assertTrue(c1.owns.contains("tagA"));
        assertTrue(c1.owns.contains("tagB"));
        assertTrue(c1.canAdd.contains("tagC"));
        assertTrue(c1.canRemove.contains("tagD"));
        assertTrue(c1.tags.contains("tagA"));
        assertTrue(c1.tags.contains("tagB"));

        System.out.println("testInitialization passed");
    }

    @Test
    public void testCreateTagSuccess() {
        TagRegistrar registrar = new TagRegistrar();
        int tagId = registrar.createTag("newTag", "clientX");
        assertTrue(tagId > 0);
        assertTrue(registrar.tagsByName.containsKey("newTag"));
        ClientDIFCPrivs client = registrar.clientsById.get("clientX");
        assertTrue(client.owns.contains("newTag"));
        System.out.println("testCreateTagSuccess passed");
    }

    @Test
    public void testCreateTagDuplicate() {
        TagRegistrar registrar = new TagRegistrar();
        registrar.createTag("dupTag", "clientX");
        int result = registrar.createTag("dupTag", "clientY");
        assertEquals(-1, result);
        System.out.println("testCreateTagDuplicate passed");
    }

    @Test
    public void testCreateTagInvalidName() {
        TagRegistrar registrar = new TagRegistrar();
        int result = registrar.createTag("invalid@tag", "clientX");
        assertEquals(-1, result);
        assertFalse(registrar.tagsByName.containsKey("invalid@tag"));
        System.out.println("testCreateTagInvalidName passed");
    }

    @Test
    public void testDestroyTag() {
        TagRegistrar registrar = new TagRegistrar();
        registrar.createTag("toDestroy", "clientX");
        int result = registrar.destroyTag("toDestroy");
        assertEquals(0, result);
        assertFalse(registrar.tagsByName.containsKey("toDestroy"));
        ClientDIFCPrivs client = registrar.clientsById.get("clientX");
        assertFalse(client.owns.contains("toDestroy"));
        System.out.println("testDestroyTag passed");
    }

    @Test
    public void testDestroyNonExistentTag() {
        TagRegistrar registrar = new TagRegistrar();
        int result = registrar.destroyTag("nonexistent");
        assertEquals(-1, result);
        System.out.println("testDestroyNonExistentTag passed");
    }

    @Test
    public void testGetTag() {
        TagRegistrar registrar = new TagRegistrar();
        int tagId = registrar.createTag("getTag", "clientX");
        int retrievedId = registrar.getTag("getTag");
        assertEquals(tagId, retrievedId);
        assertEquals(-1, registrar.getTag("nonexistent"));
        System.out.println("testGetTag passed");
    }

    @Test
    public void testAddClientPrivs() {
        TagRegistrar registrar = new TagRegistrar();
        registrar.createTag("privTag", "clientX");
        int result = registrar.addClientPrivs("clientY", "privTag", Capability.CAN_ADD);
        assertEquals(0, result);
        ClientDIFCPrivs clientY = registrar.clientsById.get("clientY");
        assertTrue(clientY.canAdd.contains("privTag"));
        System.out.println("testAddClientPrivs passed");
    }

    @Test
    public void testAddClientPrivsNonExistentTag() {
        TagRegistrar registrar = new TagRegistrar();
        int result = registrar.addClientPrivs("clientY", "nonexistent", Capability.CAN_ADD);
        assertEquals(-1, result);
        System.out.println("testAddClientPrivsNonExistentTag passed");
    }

    @Test
    public void testRemoveClientPrivs() {
        TagRegistrar registrar = new TagRegistrar();
        registrar.createTag("privTag", "clientX");
        registrar.addClientPrivs("clientY", "privTag", Capability.CAN_ADD);
        int result = registrar.removeClientPrivs("clientY", "privTag", Capability.CAN_ADD);
        assertEquals(0, result);
        ClientDIFCPrivs clientY = registrar.clientsById.get("clientY");
        assertFalse(clientY.canAdd.contains("privTag"));
        System.out.println("testRemoveClientPrivs passed");
    }

    @Test
    public void testRemoveClientPrivsNonExistentTag() {
        TagRegistrar registrar = new TagRegistrar();
        int result = registrar.removeClientPrivs("clientY", "nonexistent", Capability.CAN_ADD);
        assertEquals(-1, result);
        System.out.println("testRemoveClientPrivsNonExistentTag passed");
    }
}