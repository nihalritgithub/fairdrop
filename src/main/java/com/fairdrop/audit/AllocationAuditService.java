package com.fairdrop.audit;

import com.fairdrop.event.AllocationCreatedEvent;
import com.fairdrop.messaging.IdempotentConsumerGuard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;

/**
 * Applies {@link AllocationCreatedEvent}s to the audit log exactly once per event id, even though
 * Kafka may deliver an event more than once.
 */
@Service
public class AllocationAuditService {

    static final String CONSUMER_NAME = "allocation-audit";

    private static final Logger log = LoggerFactory.getLogger(AllocationAuditService.class);

    private final AllocationAuditJpaRepository repository;
    private final IdempotentConsumerGuard idempotencyGuard;
    private final Clock clock;

    public AllocationAuditService(
            AllocationAuditJpaRepository repository,
            IdempotentConsumerGuard idempotencyGuard,
            Clock clock) {

        this.repository = repository;
        this.idempotencyGuard = idempotencyGuard;
        this.clock = clock;
    }

    /**
     * @return {@code true} if the event was recorded, {@code false} if it was a duplicate delivery
     */
    @Transactional
    public boolean record(AllocationCreatedEvent event) {
        if (!idempotencyGuard.claim(CONSUMER_NAME, event.eventId())) {
            log.info("Skipping duplicate allocation event eventId={} dropId={}", event.eventId(), event.dropId());
            return false;
        }

        repository.save(new AllocationAuditEntity(event, clock.instant()));

        log.info("Allocation audited eventId={} dropId={} userId={} sequence={}",
                event.eventId(), event.dropId(), event.userId(), event.sequenceNumber());
        return true;
    }
}
