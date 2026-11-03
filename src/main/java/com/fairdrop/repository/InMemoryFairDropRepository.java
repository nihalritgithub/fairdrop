package com.fairdrop.repository;

import com.fairdrop.exception.DropNotFoundException;
import com.fairdrop.model.AllocationResult;
import com.fairdrop.model.Drop;
import com.fairdrop.model.QueueEntry;
import org.springframework.stereotype.Repository;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe in-memory implementation of FairDropRepository.
 *
 * Stores drops, queue entries, sequence counters and allocations in memory.
 * Data is lost when the application restarts.
 */
@Repository
public class InMemoryFairDropRepository implements FairDropRepository {

    // Drop id -> Drop.
    private final Map<String, Drop> drops = new ConcurrentHashMap<>();

    // Drop id -> (User id -> QueueEntry).
    private final Map<String, Map<String, QueueEntry>> queues = new ConcurrentHashMap<>();

    // Drop id -> Last sequence number assigned.
    private final Map<String, AtomicLong> sequences = new ConcurrentHashMap<>();

    // Drop id -> Set of users who already received inventory.
    private final Map<String, Set<String>> allocations = new ConcurrentHashMap<>();

    @Override
    public Optional<Drop> findDropById(String dropId) {
        return Optional.ofNullable(drops.get(dropId));
    }

    /**
     * Stores a new drop and initializes its queue, sequence counter
     * and allocation tracking.
     */
    @Override
    public void createDrop(Drop drop) {

        String dropId = drop.getId();

        drops.put(dropId, drop);

        queues.put(
                dropId,
                new ConcurrentHashMap<>()
        );

        sequences.put(
                dropId,
                new AtomicLong(0)
        );

        allocations.put(
                dropId,
                ConcurrentHashMap.newKeySet()
        );
    }

    /**
     * Adds a user to the queue.
     *
     * computeIfAbsent ensures that the same user receives the same
     * queue entry even if the user joins multiple times.
     */
    @Override
    public QueueEntry getOrCreateQueueEntry(
            String dropId,
            String userId) {

        Map<String, QueueEntry> queue =
                queues.get(dropId);

        AtomicLong sequence =
                sequences.get(dropId);

        return queue.computeIfAbsent(
                userId,
                id -> new QueueEntry(
                        id,
                        sequence.incrementAndGet()
                )
        );
    }

    /**
     * Finds a user's queue entry for a particular drop.
     */
    @Override
    public Optional<QueueEntry> findQueueEntry(
            String dropId,
            String userId) {

        Map<String, QueueEntry> queue =
                queues.get(dropId);

        if (queue == null) {
            return Optional.empty();
        }

        return Optional.ofNullable(
                queue.get(userId)
        );
    }

    /**
     * Allocates inventory to the next eligible user in queue order.
     *
     * Users are checked from the smallest sequence number to the largest.
     * Users who already received inventory are skipped.
     */
    @Override
    public synchronized AllocationResult allocateNext(String dropId) {

        Drop drop = drops.get(dropId);

        if (drop == null) {
            throw new DropNotFoundException(dropId);
        }

        if (!drop.hasInventory()) {
            return AllocationResult.soldOut();
        }

        Map<String, QueueEntry> dropQueue = queues.get(dropId);

        if (dropQueue == null || dropQueue.isEmpty()) {
            return AllocationResult.noEligibleUser();
        }

        Set<String> dropAllocations =
                allocations.computeIfAbsent(dropId, id -> ConcurrentHashMap.newKeySet());

        Optional<QueueEntry> nextUser =
                dropQueue.values()
                        .stream()
                        .sorted(Comparator.comparingLong(QueueEntry::getSequenceNumber))
                        .filter(entry -> !dropAllocations.contains(entry.getUserId()))
                        .findFirst();

        if (nextUser.isEmpty()) {
            return AllocationResult.noEligibleUser();
        }

        QueueEntry queueEntry = nextUser.get();

        drop.allocateOne();

        dropAllocations.add(queueEntry.getUserId());

        return AllocationResult.allocated(queueEntry, UUID.randomUUID().toString());
    }
}
