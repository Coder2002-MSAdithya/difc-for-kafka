package org.apache.kafka.server.difc;

import java.security.SecureRandom;
import java.util.Objects;
import java.util.UUID;

public final class Tag
{
    public final String tagName;
    public final int tagId;
    public final String ownerClient;

    private static final SecureRandom RNG = new SecureRandom();
    public static final String NAME_PATTERN = "^[A-Za-z0-9_-]+$";
    public static final int MAX_TAG_LENGTH = 16;

    public Tag(String tagName, String ownerClient)
    {
        this.tagName = Objects.requireNonNull(tagName, "tagName");
        this.ownerClient = Objects.requireNonNull(ownerClient, "ownerClient");

        if (tagName.isEmpty())
        {
            throw new IllegalArgumentException("tagName must not be empty");
        }

        if (tagName.length() > MAX_TAG_LENGTH)
        {
            throw new IllegalArgumentException("tagName must not exceed " + MAX_TAG_LENGTH + " characters");
        }

        if (!tagName.matches(NAME_PATTERN))
        {
            throw new IllegalArgumentException("tagName contains invalid characters; only alphanumeric, '_' and '-' allowed");
        }

        // Generate a sufficiently large unique-ish positive 31-bit id by mixing secure random,
        // current time and a UUID hash. Not strictly collision-free but very unlikely to collide.
        int rand = RNG.nextInt();
        int time = (int) (System.nanoTime() & 0x7FFFFFFF);
        int uuidHash = UUID.randomUUID().hashCode();
        int id = rand ^ time ^ uuidHash;
        this.tagId = id & 0x7FFFFFFF; // ensure positive
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof Tag)) return false;
        Tag tag = (Tag) o;
        return tagName.equals(tag.tagName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tagName);
    }

    @Override
    public String toString()
    {
        return "Tag{" + "tagName='" + tagName + '\'' + ", tagId=" + tagId + ", ownerClient='" + ownerClient + '\'' + '}';
    }
}
