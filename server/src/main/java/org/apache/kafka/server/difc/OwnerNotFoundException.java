package org.apache.kafka.server.difc;

public class OwnerNotFoundException extends TagException
{
    public OwnerNotFoundException(String message)
    {
        super(message);
    }
}
