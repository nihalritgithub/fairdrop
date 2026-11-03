package com.fairdrop.outbox;

import com.fairdrop.event.AllocationCreatedEvent;
import com.fairdrop.messaging.FairDropKafkaProperties;
import com.fairdrop.observability.CorrelationId;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;

/**
 * Writes domain events to the outbox table synchronously, inside the transaction that raised them.
 * {@link Propagation#MANDATORY} turns a call without a surrounding transaction into an error
 * instead of a silently non-atomic write.
 */
@Component
public class OutboxEventRecorder {

    private final OutboxEventJpaRepository repository;
    private final JsonMapper jsonMapper;
    private final FairDropKafkaProperties kafkaProperties;
    private final Clock clock;

    public OutboxEventRecorder(
            OutboxEventJpaRepository repository,
            JsonMapper jsonMapper,
            FairDropKafkaProperties kafkaProperties,
            Clock clock) {

        this.repository = repository;
        this.jsonMapper = jsonMapper;
        this.kafkaProperties = kafkaProperties;
        this.clock = clock;
    }

    @EventListener
    @Transactional(propagation = Propagation.MANDATORY)
    public void onAllocationCreated(AllocationCreatedEvent event) {
        repository.save(OutboxEventEntity.pending(
                event.eventId(),
                AllocationCreatedEvent.AGGREGATE_TYPE,
                event.dropId(),
                AllocationCreatedEvent.TYPE,
                kafkaProperties.allocationEventsTopic(),
                event.dropId(),
                jsonMapper.writeValueAsString(event),
                CorrelationId.current().orElse(null),
                clock.instant()));
    }
}
