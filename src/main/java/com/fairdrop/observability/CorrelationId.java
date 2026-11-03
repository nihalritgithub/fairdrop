package com.fairdrop.observability;

import org.slf4j.MDC;

import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Correlation identifier that ties together the log lines of one request, including the outbox
 * row and Kafka message it produces. Stored in the SLF4J MDC under {@link #MDC_KEY}.
 */
public final class CorrelationId {

    public static final String HTTP_HEADER = "X-Correlation-Id";
    public static final String KAFKA_HEADER = "fairdrop-correlation-id";
    public static final String MDC_KEY = "correlationId";

    // Client-supplied ids end up in logs and Kafka headers, so only short, plain tokens are accepted.
    private static final Pattern ACCEPTED_FORMAT = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    private CorrelationId() {
    }

    public static Optional<String> current() {
        return Optional.ofNullable(MDC.get(MDC_KEY));
    }

    public static String acceptOrGenerate(String candidate) {
        return isAcceptable(candidate) ? candidate : UUID.randomUUID().toString();
    }

    private static boolean isAcceptable(String candidate) {
        return candidate != null && ACCEPTED_FORMAT.matcher(candidate).matches();
    }
}
