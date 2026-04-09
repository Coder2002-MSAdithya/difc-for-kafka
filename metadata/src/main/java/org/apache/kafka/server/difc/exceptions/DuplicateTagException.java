package org.apache.kafka.server.difc.exceptions;

public class DuplicateTagException extends TagException
{
    public DuplicateTagException(String message)
    {
        super(message);
    }
}
