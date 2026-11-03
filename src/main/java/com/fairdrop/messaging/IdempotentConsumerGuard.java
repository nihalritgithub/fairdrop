package com.fairdrop.messaging;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.UUID;

/**
 * De-duplicates at-least-once Kafka deliveries. Must run inside the transaction that applies the
 * consumer's side effects, so the marker and the effects commit or roll back together.
 */
@Component
public class IdempotentConsumerGuard {

    private final ProcessedEventJpaRepository repository;
    private final Clock clock;

    public IdempotentConsumerGuard(ProcessedEventJpaRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    /**
     * @return {@code true} if the caller should apply the event, {@code false} for a duplicate
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean claim(String consumerName, UUID eventId) {
        return repository.insertIfAbsent(consumerName, eventId, clock.instant()) == 1;
    }
}
