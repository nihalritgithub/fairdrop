package com.fairdrop.repository;

import com.fairdrop.exception.DropNotFoundException;
import com.fairdrop.model.AllocationResult;
import com.fairdrop.model.Drop;
import com.fairdrop.model.QueueEntry;
import com.fairdrop.observability.FairDropMetrics;
import com.fairdrop.repository.entity.AllocationEntity;
import com.fairdrop.repository.entity.DropSequenceEntity;
import com.fairdrop.repository.entity.QueueEntryEntity;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * PostgreSQL-backed repository and the source of truth for all allocation state. Concurrency is
 * controlled with row-level locks on {@code drop_sequences} (joins) and {@code drops} (allocation).
 */
@Repository
@Primary
public class PostgresFairDropRepository implements FairDropRepository {

    private final DropJpaRepository dropJpaRepository;
    private final DropSequenceJpaRepository dropSequenceJpaRepository;
    private final QueueEntryJpaRepository queueEntryJpaRepository;
    private final AllocationJpaRepository allocationJpaRepository;
    private final FairDropMetrics metrics;

    public PostgresFairDropRepository(
            DropJpaRepository dropJpaRepository,
            DropSequenceJpaRepository dropSequenceJpaRepository,
            QueueEntryJpaRepository queueEntryJpaRepository,
            AllocationJpaRepository allocationJpaRepository,
            FairDropMetrics metrics) {

        this.dropJpaRepository = dropJpaRepository;
        this.dropSequenceJpaRepository = dropSequenceJpaRepository;
        this.queueEntryJpaRepository = queueEntryJpaRepository;
        this.allocationJpaRepository = allocationJpaRepository;
        this.metrics = metrics;
    }

    @Override
    @Transactional
    public void createDrop(Drop drop) {
        dropJpaRepository.save(drop);
        dropSequenceJpaRepository.save(new DropSequenceEntity(drop.getId()));
    }

    @Override
    public Optional<Drop> findDropById(String dropId) {
        return dropJpaRepository.findById(dropId);
    }

    @Override
    @Transactional
    public QueueEntry getOrCreateQueueEntry(String dropId, String userId) {

        Optional<QueueEntryEntity> existingEntry =
                queueEntryJpaRepository.findByDropIdAndUserId(dropId, userId);

        if (existingEntry.isPresent()) {
            return toQueueEntry(existingEntry.get());
        }

        DropSequenceEntity sequence =
                metrics.joinSequenceLockWait()
                        .record(() -> dropSequenceJpaRepository.findByDropIdForUpdate(dropId))
                        .orElseThrow(() ->
                                new IllegalStateException("Sequence not initialized for drop: " + dropId));

        // Re-check under the sequence lock: a concurrent join for the same user may have just committed.
        existingEntry = queueEntryJpaRepository.findByDropIdAndUserId(dropId, userId);

        if (existingEntry.isPresent()) {
            return toQueueEntry(existingEntry.get());
        }

        long sequenceNumber = sequence.nextSequence();

        dropSequenceJpaRepository.save(sequence);

        QueueEntryEntity savedEntry =
                queueEntryJpaRepository.save(new QueueEntryEntity(dropId, userId, sequenceNumber));

        return toQueueEntry(savedEntry);
    }

    @Override
    public Optional<QueueEntry> findQueueEntry(String dropId, String userId) {
        return queueEntryJpaRepository
                .findByDropIdAndUserId(dropId, userId)
                .map(PostgresFairDropRepository::toQueueEntry);
    }

    /**
     * Locks the drop row before reading its inventory, so concurrent allocations are serialized and
     * can never oversell. The drop must not be loaded earlier in the same persistence context,
     * otherwise Hibernate would hand back that unlocked snapshot instead of the locked row state.
     */
    @Override
    @Transactional
    public AllocationResult allocateNext(String dropId) {

        Drop drop =
                metrics.allocationLockWait()
                        .record(() -> dropJpaRepository.findByIdForUpdate(dropId))
                        .orElseThrow(() -> new DropNotFoundException(dropId));

        if (!drop.hasInventory()) {
            return AllocationResult.soldOut();
        }

        Optional<QueueEntryEntity> nextQueueEntry =
                queueEntryJpaRepository.findNextUnallocatedUser(dropId);

        if (nextQueueEntry.isEmpty()) {
            return AllocationResult.noEligibleUser();
        }

        QueueEntryEntity queueEntry = nextQueueEntry.get();

        drop.allocateOne();

        dropJpaRepository.save(drop);

        AllocationEntity allocation =
                allocationJpaRepository.save(new AllocationEntity(dropId, queueEntry.getUserId()));

        return AllocationResult.allocated(toQueueEntry(queueEntry), allocation.getId());
    }

    private static QueueEntry toQueueEntry(QueueEntryEntity entity) {
        return new QueueEntry(entity.getUserId(), entity.getSequenceNumber());
    }
}
