package com.fairdrop.messaging;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Kafka header names attached to every FairDrop event.
 */
public final class EventHeaders {

    public static final String EVENT_ID = "fairdrop-event-id";
    public static final String EVENT_TYPE = "fairdrop-event-type";

    private EventHeaders() {
    }

    public static Optional<String> lastValue(Headers headers, String name) {
        Header header = headers.lastHeader(name);
        return header == null || header.value() == null
                ? Optional.empty()
                : Optional.of(new String(header.value(), StandardCharsets.UTF_8));
    }
}
