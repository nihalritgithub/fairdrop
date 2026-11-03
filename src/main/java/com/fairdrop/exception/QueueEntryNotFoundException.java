package com.fairdrop.exception;

/**
 * Thrown when a user's queue position is requested for a drop they never joined.
 * Mapped to HTTP 404 by {@link GlobalExceptionHandler}.
 */
public class QueueEntryNotFoundException extends RuntimeException {

    public QueueEntryNotFoundException(String userId) {
        super("User has not joined this drop: " + userId);
    }
}
