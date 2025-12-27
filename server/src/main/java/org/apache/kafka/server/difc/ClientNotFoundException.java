package org.apache.kafka.server.difc;

public class ClientNotFoundException extends TagException
{
    public ClientNotFoundException(String message)
    {
        super(message);
    }
}
