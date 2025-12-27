package org.apache.kafka.server.difc;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TagRegistrarTest {

    @Test
    void testInitialization()
    {
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
    }

    @Test
    void testCreateTagSuccess()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.registerClient("clientX");

        int tagId = registrar.createTag("newTag", "clientX");
        assertTrue(tagId > 0);
        assertTrue(registrar.hasTag("newTag"));

        ClientDIFCPrivs client = registrar.getClientPrivs("clientX");
        assertTrue(client.owns.contains("newTag"));
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
        assertFalse(registrar.hasTag("invalid@tag"));
    }

    @Test
    void testDestroyTag()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.registerClient("clientX");
        registrar.createTag("toDestroy", "clientX");

        int result = registrar.destroyTag("toDestroy");
        assertEquals(0, result);
        assertFalse(registrar.hasTag("toDestroy"));

        ClientDIFCPrivs client = registrar.getClientPrivs("clientX");
        assertFalse(client.owns.contains("toDestroy"));
    }

    @Test
    void testDestroyNonExistentTag()
    {
        TagRegistrar registrar = new TagRegistrar();
        assertThrows(TagNotFoundException.class,
                () -> registrar.destroyTag("nonexistent"));
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

        ClientDIFCPrivs clientY = registrar.getClientPrivs("clientY");
        assertTrue(clientY.canAdd.contains("privTag"));
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

        ClientDIFCPrivs clientY = registrar.getClientPrivs("clientY");
        assertFalse(clientY.canAdd.contains("privTag"));
    }

    @Test
    void testRemoveClientPrivsNonExistentTag()
    {
        TagRegistrar registrar = new TagRegistrar();

        assertThrows(TagNotFoundException.class,
                () -> registrar.removeClientPrivs("clientY", "nonexistent", Capability.CAN_ADD));
    }

    @Test
    public void testCanClientReceiveSameClientEmptyMessage()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.initialize();

        Set<String> messageTags = new HashSet<>();
        assertTrue(registrar.canClientReceive("client1", "client1", messageTags));
    }

    @Test
    public void testCanClientReceiveUnionSubset()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.registerClient("client1");

        // Setup: Add tags to clients
        registrar.createTag("tagX", "client1");
        registrar.createTag("tagY", "client1");
        registrar.addClientPrivs("client2", "tagX", Capability.CAN_ADD); // Ensure client2 is created

        ClientDIFCPrivs sender = registrar.getClientPrivs("client1");
        ClientDIFCPrivs receiver = registrar.getClientPrivs("client2");

        sender.tags.add("tagX");
        receiver.tags.add("tagX");
        receiver.tags.add("tagY");

        Set<String> messageTags = new HashSet<>(Arrays.asList("tagY"));
        assertTrue(registrar.canClientReceive("client1", "client2", messageTags));
    }

    @Test
    public void testCanClientReceiveUnionNotSubset()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.registerClient("client1");

        // Setup: Add tags to clients
        registrar.createTag("tagX", "client1");
        registrar.createTag("tagY", "client1");
        registrar.addClientPrivs("client2", "tagX", Capability.CAN_ADD); // Ensure client2 is created

        ClientDIFCPrivs sender = registrar.getClientPrivs("client1");
        ClientDIFCPrivs receiver = registrar.getClientPrivs("client2");

        sender.tags.add("tagX");
        receiver.tags.add("tagX");

        Set<String> messageTags = new HashSet<>(Arrays.asList("tagY"));
        assertFalse(registrar.canClientReceive("client1", "client2", messageTags));
    }

    @Test
    void testCreateTagNonExistingOwner()
    {
        TagRegistrar registrar = new TagRegistrar();

        assertThrows(OwnerNotFoundException.class,
                () -> registrar.createTag("newTag", "unknown"));
        assertFalse(registrar.hasTag("newTag"));
    }

    @Test
    void testRegisterClientSuccess()
    {
        TagRegistrar registrar = new TagRegistrar();
        int result = registrar.registerClient("newClient");
        assertEquals(0, result);
        assertNotNull(registrar.getClientPrivs("newClient"));
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
        assertFalse(registrar.canClientReceive("nonexistent", "client1", messageTags));
        assertFalse(registrar.canClientReceive("client1", "nonexistent", messageTags));
    }

    @Test
    public void testCanClientReceiveWithUnknownMessageTag()
    {
        TagRegistrar registrar = new TagRegistrar();
        registrar.initialize();

        Set<String> messageTags = new HashSet<>(Arrays.asList("unknownTag"));
        // client1 tags: tagA, tagB; union: tagA, tagB, unknownTag; not subset of itself unless it has unknownTag
        assertFalse(registrar.canClientReceive("client1", "client1", messageTags));
    }
}
