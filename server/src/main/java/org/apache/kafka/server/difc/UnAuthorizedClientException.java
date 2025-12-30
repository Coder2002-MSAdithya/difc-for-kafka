package org.apache.kafka.server.difc;

public class UnAuthorizedClientException extends TagException {
    public UnAuthorizedClientException(String message) {
        super(message);
    }
}
