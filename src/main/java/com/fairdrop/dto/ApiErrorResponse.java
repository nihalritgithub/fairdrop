package com.fairdrop.dto;

import java.util.Map;

/**
 * Uniform error body returned by {@link com.fairdrop.exception.GlobalExceptionHandler}, carrying a
 * machine-readable code, a human-readable message, and optional per-field validation errors.
 */
public class ApiErrorResponse {

    private final String code;
    private final String message;
    private final Map<String, String> errors;

    /**
     * @param code    stable error code clients can branch on (e.g. {@code DROP_NOT_FOUND})
     * @param message human-readable description of the error
     * @param errors  field name to validation message; empty when not a validation error
     */
    public ApiErrorResponse(String code, String message, Map<String, String> errors) {
        this.code = code;
        this.message = message;
        this.errors = errors;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public Map<String, String> getErrors() {
        return errors;
    }
}
