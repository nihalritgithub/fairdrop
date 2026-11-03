package com.fairdrop.messaging;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.UUID;

public interface ProcessedEventJpaRepository
        extends JpaRepository<ProcessedEventEntity, ProcessedEventEntity.Key> {

    /**
     * Inserts the marker unless it already exists. A concurrent duplicate blocks on the primary key
     * until the first transaction finishes and then inserts nothing.
     *
     * @return 1 if this call claimed the event, 0 if it had already been processed
     */
    @Modifying
    @Query(
            value = """
                    INSERT INTO processed_events (consumer_name, event_id, processed_at)
                    VALUES (:consumerName, :eventId, :processedAt)
                    ON CONFLICT DO NOTHING
                    """,
            nativeQuery = true
    )
    int insertIfAbsent(
            @Param("consumerName") String consumerName,
            @Param("eventId") UUID eventId,
            @Param("processedAt") Instant processedAt);
}
