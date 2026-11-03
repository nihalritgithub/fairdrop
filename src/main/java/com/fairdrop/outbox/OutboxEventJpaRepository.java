package com.fairdrop.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OutboxEventJpaRepository extends JpaRepository<OutboxEventEntity, UUID> {

    /**
     * Claims due events in creation order. {@code SKIP LOCKED} lets several application instances
     * relay concurrently without ever handing the same row to two publishers.
     */
    @Query(
            value = """
                    SELECT *
                    FROM outbox_events
                    WHERE status = 'PENDING'
                      AND next_attempt_at <= :now
                    ORDER BY created_at
                    LIMIT :limit
                    FOR UPDATE SKIP LOCKED
                    """,
            nativeQuery = true
    )
    List<OutboxEventEntity> lockPublishableBatch(@Param("now") Instant now, @Param("limit") int limit);

    long countByStatus(OutboxStatus status);

    @Query("""
            SELECT MIN(o.createdAt)
            FROM OutboxEventEntity o
            WHERE o.status = :status
            """)
    Optional<Instant> findOldestCreatedAt(@Param("status") OutboxStatus status);

    @Modifying
    @Query("""
            DELETE FROM OutboxEventEntity o
            WHERE o.status = com.fairdrop.outbox.OutboxStatus.PUBLISHED
              AND o.publishedAt < :cutoff
            """)
    int deletePublishedBefore(@Param("cutoff") Instant cutoff);
}
