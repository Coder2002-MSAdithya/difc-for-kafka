package org.apache.kafka.server.difc;

public class DuplicateTagException extends TagException
{
    public DuplicateTagException(String message)
    {
        super(message);
    }
}
