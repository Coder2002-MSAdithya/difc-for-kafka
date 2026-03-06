package org.apache.kafka.server.difc.exceptions;

public class ClientExistsException extends TagException
{
    public ClientExistsException(String message)
    {
        super(message);
    }
}
