package com.fairdrop.messaging;

/**
 * A consumed record that can never be processed, e.g. unparseable JSON or missing required fields.
 * Not retried: the Kafka error handler sends it straight to the dead-letter topic.
 */
public class MalformedEventException extends RuntimeException {

    public MalformedEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
