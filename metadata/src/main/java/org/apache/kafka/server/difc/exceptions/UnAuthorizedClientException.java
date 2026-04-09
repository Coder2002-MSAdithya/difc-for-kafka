package org.apache.kafka.server.difc.exceptions;

public class UnAuthorizedClientException extends TagException {
    public UnAuthorizedClientException(String message) {
        super(message);
    }
}
