package com.fairdrop.service;

import com.fairdrop.cache.DropCache;
import com.fairdrop.event.AllocationCreatedEvent;
import com.fairdrop.exception.DropNotFoundException;
import com.fairdrop.exception.QueueEntryNotFoundException;
import com.fairdrop.model.AllocationResult;
import com.fairdrop.model.AllocationStatus;
import com.fairdrop.model.Drop;
import com.fairdrop.model.QueueEntry;
import com.fairdrop.observability.FairDropMetrics;
import com.fairdrop.repository.FairDropRepository;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Business logic for FairDrop.
 *
 * Handles drop creation, queue operations and fair inventory allocation.
 * The service validates business prerequisites and delegates persistence
 * and concurrency-sensitive work to the repository layer.
 */
@Service
public class DropService {

    private static final Logger log = LoggerFactory.getLogger(DropService.class);

    private final FairDropRepository repository;
    private final DropCache dropCache;
    private final ApplicationEventPublisher eventPublisher;
    private final FairDropMetrics metrics;

    public DropService(
            FairDropRepository repository,
            DropCache dropCache,
            ApplicationEventPublisher eventPublisher,
            FairDropMetrics metrics) {

        this.repository = repository;
        this.dropCache = dropCache;
        this.eventPublisher = eventPublisher;
        this.metrics = metrics;
    }

    /**
     * Creates a new drop with a random UUID.
     *
     * @param name      display name of the drop
     * @param inventory number of units available
     * @return newly created drop
     */
    public Drop createDrop(String name, int inventory) {

        String dropId = UUID.randomUUID().toString();

        Drop drop = new Drop(dropId, name, inventory);

        repository.createDrop(drop);

        log.info("Drop created dropId={} inventory={}", dropId, inventory);

        return drop;
    }

    /**
     * Returns an existing drop, reading Redis first and falling back to PostgreSQL on a miss.
     * The returned snapshot is for display and existence checks only; allocation always re-reads
     * the locked row.
     *
     * @throws DropNotFoundException if the drop does not exist
     */
    public Drop getDrop(String dropId) {

        return dropCache
                .find(dropId)
                .orElseGet(() -> {
                    Drop drop = repository
                            .findDropById(dropId)
                            .orElseThrow(() -> new DropNotFoundException(dropId));

                    dropCache.put(drop);

                    return drop;
                });
    }

    /**
     * Adds a user to a drop's queue.
     *
     * If the user already joined, their existing queue entry is returned.
     *
     * @throws DropNotFoundException if the drop does not exist
     */
    public QueueEntry joinDrop(String dropId, String userId) {

        getDrop(dropId);

        QueueEntry entry =
                metrics.joinDuration().record(() -> repository.getOrCreateQueueEntry(dropId, userId));

        log.debug("User joined drop dropId={} userId={} sequence={}", dropId, userId, entry.getSequenceNumber());

        return entry;
    }

    /**
     * Returns the queue entry of a user who already joined.
     *
     * @throws DropNotFoundException       if the drop does not exist
     * @throws QueueEntryNotFoundException if the user has not joined
     */
    public QueueEntry getQueueEntry(String dropId, String userId) {

        getDrop(dropId);

        return repository
                .findQueueEntry(dropId, userId)
                .orElseThrow(() -> new QueueEntryNotFoundException(userId));
    }

    /**
     * Allocates inventory to the next eligible user in queue order.
     *
     * Possible outcomes are represented by AllocationResult:
     *
     * ALLOCATED
     * SOLD_OUT
     * NO_ELIGIBLE_USER
     *
     * A successful allocation raises {@link AllocationCreatedEvent} inside this transaction, so the
     * outbox record commits or rolls back together with the allocation itself.
     *
     * @throws DropNotFoundException if the drop does not exist
     */
    @Transactional
    public AllocationResult allocateNext(String dropId) {

        Timer.Sample sample = metrics.startTimer();

        AllocationResult result = repository.allocateNext(dropId);

        metrics.allocationCompleted(sample, result.getStatus());

        if (result.getStatus() != AllocationStatus.ALLOCATED) {
            log.debug("Allocation not possible dropId={} outcome={}", dropId, result.getStatus());
            return result;
        }

        QueueEntry winner = result.getQueueEntry().orElseThrow();

        AllocationCreatedEvent event =
                AllocationCreatedEvent.of(
                        result.getAllocationId().orElseThrow(),
                        dropId,
                        winner.getUserId(),
                        winner.getSequenceNumber(),
                        Instant.now());

        eventPublisher.publishEvent(event);

        log.info("Allocation created dropId={} userId={} sequence={} eventId={}",
                dropId, winner.getUserId(), winner.getSequenceNumber(), event.eventId());

        return result;
    }
}
