package org.apache.kafka.server.difc;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ClientDIFCPrivsTest {

    @Test
    public void testCreation() {
        ClientDIFCPrivs privs = new ClientDIFCPrivs("client1");
        assertEquals("client1", privs.clientId);
        assertTrue(privs.tags.isEmpty());
        assertTrue(privs.canAdd.isEmpty());
        assertTrue(privs.canRemove.isEmpty());
        assertTrue(privs.owns.isEmpty());
        System.out.println("testCreation passed");
    }

    @Test
    public void testNullClientId() {
        assertThrows(NullPointerException.class, () -> new ClientDIFCPrivs(null));
        System.out.println("testNullClientId passed (expected exception thrown)");
    }

    @Test
    public void testSetOperations() {
        ClientDIFCPrivs privs = new ClientDIFCPrivs("client1");

        privs.tags.add("tagA");
        assertTrue(privs.tags.contains("tagA"));
        assertEquals(1, privs.tags.size());

        privs.canAdd.add("tagB");
        assertTrue(privs.canAdd.contains("tagB"));

        privs.canRemove.add("tagC");
        assertTrue(privs.canRemove.contains("tagC"));

        privs.owns.add("tagD");
        assertTrue(privs.owns.contains("tagD"));

        privs.tags.remove("tagA");
        assertFalse(privs.tags.contains("tagA"));

        System.out.println("testSetOperations passed");
    }

    @Test
    public void testToString() {
        ClientDIFCPrivs privs = new ClientDIFCPrivs("client1");
        privs.tags.add("tagA");
        String str = privs.toString();
        assertTrue(str.contains("clientId='client1'"));
        assertTrue(str.contains("tags=[tagA]"));
        System.out.println("testToString passed");
    }
}