package com.fairdrop.outbox;

/**
 * Lifecycle of an outbox event: waiting to be published, acknowledged by Kafka, or parked after
 * exhausting its publish attempts (requires operator attention).
 */
public enum OutboxStatus {
    PENDING,
    PUBLISHED,
    FAILED
}
