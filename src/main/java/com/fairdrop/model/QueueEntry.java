package com.fairdrop.model;

/**
 * A user's place in a drop's queue. The sequence number is assigned once, when the user first
 * joins, and determines their order for claiming inventory.
 */
public class QueueEntry {

    private String userId;
    private long sequenceNumber;

    public QueueEntry(String userId, long sequenceNumber) {
        this.userId = userId;
        this.sequenceNumber = sequenceNumber;
    }

    public String getUserId() {
        return userId;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }
}
