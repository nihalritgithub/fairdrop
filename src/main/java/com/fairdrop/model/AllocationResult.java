package com.fairdrop.model;

import java.util.Optional;

/**
 * Outcome of a single allocate-next attempt. Only {@link AllocationStatus#ALLOCATED} results carry
 * the winning queue entry and the identifier of the persisted allocation.
 */
public class AllocationResult {

    private final AllocationStatus status;
    private final QueueEntry queueEntry;
    private final String allocationId;

    private AllocationResult(AllocationStatus status, QueueEntry queueEntry, String allocationId) {
        this.status = status;
        this.queueEntry = queueEntry;
        this.allocationId = allocationId;
    }

    public static AllocationResult allocated(QueueEntry queueEntry, String allocationId) {
        return new AllocationResult(AllocationStatus.ALLOCATED, queueEntry, allocationId);
    }

    public static AllocationResult soldOut() {
        return new AllocationResult(AllocationStatus.SOLD_OUT, null, null);
    }

    public static AllocationResult noEligibleUser() {
        return new AllocationResult(AllocationStatus.NO_ELIGIBLE_USER, null, null);
    }

    public AllocationStatus getStatus() {
        return status;
    }

    public Optional<QueueEntry> getQueueEntry() {
        return Optional.ofNullable(queueEntry);
    }

    public Optional<String> getAllocationId() {
        return Optional.ofNullable(allocationId);
    }
}
