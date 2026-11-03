package com.fairdrop.repository;

import com.fairdrop.model.AllocationResult;
import com.fairdrop.model.Drop;
import com.fairdrop.model.QueueEntry;

import java.util.Optional;

/**
 * Storage abstraction for drops and their per-drop user queues. Implementations must assign
 * queue sequence numbers safely under concurrent joins.
 */
public interface FairDropRepository {

    /**
     * Stores a new drop and initializes an empty queue for it.
     *
     * @param drop the drop to store
     */
    void createDrop(Drop drop);

    /**
     * @param dropId identifier of the drop
     * @return the drop, or empty if no drop has that id
     */
    Optional<Drop> findDropById(String dropId);

    /**
     * Returns the user's existing queue entry for the drop, or atomically creates one with the
     * next sequence number if the user has not joined yet.
     *
     * @param dropId identifier of an existing drop
     * @param userId identifier of the user joining
     * @return the user's queue entry
     */
    QueueEntry getOrCreateQueueEntry(String dropId, String userId);

    /**
     * @param dropId identifier of the drop
     * @param userId identifier of the user
     * @return the user's queue entry, or empty if the drop or entry does not exist
     */
    Optional<QueueEntry> findQueueEntry(String dropId, String userId);

    /**
     * Allocates one unit of inventory to the earliest queued user who has not been allocated yet.
     *
     * @param dropId identifier of the drop
     * @return the allocation outcome
     * @throws com.fairdrop.exception.DropNotFoundException if the drop does not exist
     */
    AllocationResult allocateNext(String dropId);
}
