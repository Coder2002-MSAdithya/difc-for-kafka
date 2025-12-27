package org.apache.kafka.server.difc;

public class ClientExistsException extends TagException
{
    public ClientExistsException(String message)
    {
        super(message);
    }
}
