package org.apache.kafka.server.difc;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class CapabilityTest {

    @Test
    public void testEnumValues() {
        Capability[] values = Capability.values();
        assertEquals(2, values.length);
        assertEquals(Capability.CAN_ADD, values[0]);
        assertEquals(Capability.CAN_REMOVE, values[1]);
        System.out.println("testEnumValues passed");
    }

    @Test
    public void testValueOf() {
        assertEquals(Capability.CAN_ADD, Capability.valueOf("CAN_ADD"));
        assertEquals(Capability.CAN_REMOVE, Capability.valueOf("CAN_REMOVE"));
        System.out.println("testValueOf passed");
    }
}