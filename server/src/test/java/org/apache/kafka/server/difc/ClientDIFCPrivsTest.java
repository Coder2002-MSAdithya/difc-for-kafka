package org.apache.kafka.server.difc;

import org.apache.kafka.server.difc.exceptions.NullInputException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ClientDIFCPrivsTest {

    @Test
    public void testCreation() {
        ClientDIFCPrivs privs = new ClientDIFCPrivs("client1");
        assertEquals("client1", privs.getClientId());
        assertTrue(privs.getTags().isEmpty());
        assertTrue(privs.getAddCapabilities().isEmpty());
        assertTrue(privs.getRemoveCapabilities().isEmpty());
        assertTrue(privs.getOwnedTags().isEmpty());
        System.out.println("testCreation passed");
    }

    @Test
    public void testNullClientId() {
        assertThrows(NullInputException.class, () -> new ClientDIFCPrivs((String) null));
        System.out.println("testNullClientId passed (expected exception thrown)");
    }

    @Test
    public void testSetOperations() {
        ClientDIFCPrivs privs = new ClientDIFCPrivs("client1");

        privs.addTag("tagA");
        assertTrue(privs.getTags().contains("tagA"));
        assertEquals(1, privs.getTags().size());

        privs.addCapability("tagB", Capability.CAN_ADD);
        assertTrue(privs.getAddCapabilities().contains("tagB"));

        privs.addCapability("tagC", Capability.CAN_REMOVE);
        assertTrue(privs.getRemoveCapabilities().contains("tagC"));

        privs.addOwnership("tagD");
        assertTrue(privs.getOwnedTags().contains("tagD"));

        privs.removeTag("tagA");
        assertFalse(privs.getTags().contains("tagA"));

        System.out.println("testSetOperations passed");
    }

    @Test
    public void testToString() {
        ClientDIFCPrivs privs = new ClientDIFCPrivs("client1");
        privs.addTag("tagA");
        String str = privs.toString();
        assertTrue(str.contains("clientId='client1'"));
        assertTrue(str.contains("tags=[tagA]"));
        System.out.println("testToString passed");
    }
}