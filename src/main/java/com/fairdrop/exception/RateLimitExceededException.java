package com.fairdrop.exception;

/**
 * Thrown when a user exceeds the join rate limit. Mapped to HTTP 429 by {@link GlobalExceptionHandler}.
 */
public class RateLimitExceededException extends RuntimeException {

    public RateLimitExceededException() {
        super("Too many join requests. Please try again later.");
    }
}
