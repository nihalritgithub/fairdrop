package com.fairdrop.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Domain event raised when inventory has been allocated to a queued user. It is published in-process
 * inside the allocation transaction, stored in the transactional outbox and later delivered to Kafka
 * with the same {@link #eventId}, which consumers use for de-duplication.
 */
public record AllocationCreatedEvent(
        UUID eventId,
        Instant occurredAt,
        String allocationId,
        String dropId,
        String userId,
        long sequenceNumber) {

    public static final String TYPE = "AllocationCreated";
    public static final String AGGREGATE_TYPE = "Drop";

    public static AllocationCreatedEvent of(
            String allocationId,
            String dropId,
            String userId,
            long sequenceNumber,
            Instant occurredAt) {

        return new AllocationCreatedEvent(
                UUID.randomUUID(),
                occurredAt,
                allocationId,
                dropId,
                userId,
                sequenceNumber);
    }
}
