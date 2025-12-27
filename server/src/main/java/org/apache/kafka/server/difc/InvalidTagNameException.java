package org.apache.kafka.server.difc;

public class InvalidTagNameException extends TagException
{
    public InvalidTagNameException(String message)
    {
        super(message);
    }
}
