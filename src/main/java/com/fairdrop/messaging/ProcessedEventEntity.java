package com.fairdrop.messaging;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * Durable record that a consumer has applied an event's side effects. Rows are inserted through
 * {@link ProcessedEventJpaRepository#insertIfAbsent} in the consumer's own transaction.
 */
@Entity
@Table(name = "processed_events")
public class ProcessedEventEntity {

    @EmbeddedId
    private Key key;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected ProcessedEventEntity() {
    }

    public Key getKey() {
        return key;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    @Embeddable
    public record Key(
            @Column(name = "consumer_name", nullable = false) String consumerName,
            @Column(name = "event_id", nullable = false) UUID eventId) {
    }
}
