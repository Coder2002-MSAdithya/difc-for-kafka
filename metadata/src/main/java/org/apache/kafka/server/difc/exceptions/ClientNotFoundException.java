package org.apache.kafka.server.difc.exceptions;

public class ClientNotFoundException extends TagException
{
    public ClientNotFoundException(String message)
    {
        super(message);
    }
}
