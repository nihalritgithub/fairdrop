package com.fairdrop.exception;

import com.fairdrop.dto.ApiErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.validation.method.ParameterValidationResult;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Translates exceptions thrown by FairDrop controllers into consistent {@link ApiErrorResponse}
 * bodies with the appropriate HTTP status. Infrastructure exceptions never leak to clients.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /**
     * Handles request-body validation failures, collecting each rejected field and its message.
     *
     * @param exception validation failure raised for a {@code @Valid} request body
     * @return HTTP 400 with per-field error details
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiErrorResponse> handleValidationException(MethodArgumentNotValidException exception) {
        // LinkedHashMap keeps field errors in the order they were reported.
        Map<String, String> errors = new LinkedHashMap<>();
        exception.getBindingResult()
                .getFieldErrors()
                .forEach(error -> errors.put(error.getField(), error.getDefaultMessage()));

        return error(HttpStatus.BAD_REQUEST, "VALIDATION_FAILED", "Request validation failed", errors);
    }

    /**
     * Handles constraint violations on path variables and request parameters.
     *
     * @param exception validation failure raised for controller method parameters
     * @return HTTP 400 with per-parameter error details
     */
    @ExceptionHandler(HandlerMethodValidationException.class)
    public ResponseEntity<ApiErrorResponse> handleParameterValidation(HandlerMethodValidationException exception) {
        Map<String, String> errors = new LinkedHashMap<>();
        for (ParameterValidationResult result : exception.getParameterValidationResults()) {
            String parameter = result.getMethodParameter().getParameterName();
            result.getResolvableErrors()
                    .forEach(error -> errors.putIfAbsent(String.valueOf(parameter), error.getDefaultMessage()));
        }

        return error(HttpStatus.BAD_REQUEST, "VALIDATION_FAILED", "Request validation failed", errors);
    }

    /**
     * Handles requests that reference an unknown drop.
     *
     * @param exception the missing-drop exception
     * @return HTTP 404 with code {@code DROP_NOT_FOUND}
     */
    @ExceptionHandler(DropNotFoundException.class)
    public ResponseEntity<ApiErrorResponse> handleDropNotFound(DropNotFoundException exception) {
        return error(HttpStatus.NOT_FOUND, "DROP_NOT_FOUND", exception.getMessage(), Map.of());
    }

    /**
     * Handles lookups for a user who has not joined the requested drop.
     *
     * @param exception the missing-queue-entry exception
     * @return HTTP 404 with code {@code QUEUE_ENTRY_NOT_FOUND}
     */
    @ExceptionHandler(QueueEntryNotFoundException.class)
    public ResponseEntity<ApiErrorResponse> handleQueueEntryNotFound(QueueEntryNotFoundException exception) {
        return error(HttpStatus.NOT_FOUND, "QUEUE_ENTRY_NOT_FOUND", exception.getMessage(), Map.of());
    }

    /**
     * Handles joins rejected because the rate limiter could not be consulted (fail closed).
     *
     * @param exception the limiter outage
     * @return HTTP 503 with code {@code RATE_LIMITER_UNAVAILABLE}
     */
    @ExceptionHandler(RateLimiterUnavailableException.class)
    public ResponseEntity<ApiErrorResponse> handleRateLimiterUnavailable(RateLimiterUnavailableException exception) {
        return error(HttpStatus.SERVICE_UNAVAILABLE, "RATE_LIMITER_UNAVAILABLE", exception.getMessage(), Map.of());
    }

    /**
     * Handles joins over the per-user limit.
     *
     * @param exception the limit violation
     * @return HTTP 429 with code {@code RATE_LIMIT_EXCEEDED}
     */
    @ExceptionHandler(RateLimitExceededException.class)
    public ResponseEntity<ApiErrorResponse> handleRateLimitExceeded(RateLimitExceededException exception) {
        return error(HttpStatus.TOO_MANY_REQUESTS, "RATE_LIMIT_EXCEEDED", exception.getMessage(), Map.of());
    }

    /**
     * Handles transient database conditions such as connection-pool exhaustion, lock timeouts or
     * deadlocks. The transaction has been rolled back, so the client can safely retry.
     *
     * @param exception the transient failure
     * @return HTTP 503 with code {@code TEMPORARILY_UNAVAILABLE}
     */
    @ExceptionHandler({TransientDataAccessException.class, CannotCreateTransactionException.class})
    public ResponseEntity<ApiErrorResponse> handleTransientDatabaseFailure(RuntimeException exception) {
        log.warn("Transient database failure cause={}", exception.getMessage());
        return error(
                HttpStatus.SERVICE_UNAVAILABLE,
                "TEMPORARILY_UNAVAILABLE",
                "The service is busy. Please try again shortly.",
                Map.of());
    }

    private static ResponseEntity<ApiErrorResponse> error(
            HttpStatus status,
            String code,
            String message,
            Map<String, String> errors) {

        return ResponseEntity.status(status).body(new ApiErrorResponse(code, message, errors));
    }
}
