package org.apache.kafka.server.difc;

import org.apache.kafka.server.difc.exceptions.InvalidTagNameException;
import org.apache.kafka.server.difc.exceptions.NullInputException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class TagTest {

    @Test
    public void testValidTagCreation() {
        try {
            Tag tag = new Tag("valid_tag-1", "owner1");
            assertNotNull(tag.tagName);
            assertEquals("valid_tag-1", tag.tagName);
            assertNotNull(tag.ownerClient);
            assertEquals("owner1", tag.ownerClient);
            assertTrue(tag.tagId > 0);
            System.out.println("testValidTagCreation passed");
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testEmptyTagName() {
        assertThrows(InvalidTagNameException.class, () -> new Tag("", "owner1"));
        System.out.println("testEmptyTagName passed (expected exception thrown)");
    }

    @Test
    public void testTooLongTagName() {
        String longName = "this_is_way_too_long_for_a_tag_name";
        assertThrows(InvalidTagNameException.class, () -> new Tag(longName, "owner1"));
        System.out.println("testTooLongTagName passed (expected exception thrown)");
    }

    @Test
    public void testInvalidCharactersInTagName() {
        assertThrows(InvalidTagNameException.class, () -> new Tag("invalid@tag", "owner1"));
        System.out.println("testInvalidCharactersInTagName passed (expected exception thrown)");
    }

    @Test
    public void testNullTagName() {
        assertThrows(NullInputException.class, () -> new Tag(null, "owner1"));
        System.out.println("testNullTagName passed (expected exception thrown)");
    }

    @Test
    public void testNullOwnerClient() {
        assertThrows(NullInputException.class, () -> new Tag("valid_tag", null));
        System.out.println("testNullOwnerClient passed (expected exception thrown)");
    }

    @Test
    public void testEqualsAndHashCode() {
        Tag tag1 = new Tag("tagA", "owner1");
        Tag tag2 = new Tag("tagA", "owner2");
        Tag tag3 = new Tag("tagB", "owner1");

        assertEquals(tag1, tag2);
        assertEquals(tag1.hashCode(), tag2.hashCode());
        assertNotEquals(tag1, tag3);
        assertNotEquals(tag1.hashCode(), tag3.hashCode());

        System.out.println("testEqualsAndHashCode passed");
    }

    @Test
    public void testToString() {
        Tag tag = new Tag("tagA", "owner1");
        String str = tag.toString();
        assertTrue(str.contains("tagName='tagA'"));
        assertTrue(str.contains("ownerClient='owner1'"));
        assertTrue(str.contains("tagId="));
        System.out.println("testToString passed");
    }
}