package com.fairdrop.exception;

/**
 * Thrown when the join rate limiter cannot reach Redis. Joins fail closed with HTTP 503 instead of
 * bypassing traffic protection. Mapped by {@link GlobalExceptionHandler}.
 */
public class RateLimiterUnavailableException extends RuntimeException {

    public RateLimiterUnavailableException(Throwable cause) {
        super("Join service is temporarily unavailable. Please try again later.", cause);
    }
}
