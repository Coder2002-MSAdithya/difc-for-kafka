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

    public Tag(String tagName, String ownerClient)
    {
        this.tagName = tagName;
        this.ownerClient = ownerClient;

        int rand = RNG.nextInt();
        int time = (int) (System.nanoTime() & 0x7FFFFFFF);
        int uuidHash = UUID.randomUUID().hashCode();
        int id = rand ^ time ^ uuidHash;
        this.tagId = id & 0x7FFFFFFF; // ensure positive
    }

    @Override
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