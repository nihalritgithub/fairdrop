package com.fairdrop.exception;

/**
 * Thrown when an operation references a drop id that does not exist.
 * Mapped to HTTP 404 by {@link GlobalExceptionHandler}.
 */
public class DropNotFoundException extends RuntimeException {

    public DropNotFoundException(String dropId) {
        super("Drop not found: " + dropId);
    }
}
