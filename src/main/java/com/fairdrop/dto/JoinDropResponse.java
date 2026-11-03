package com.fairdrop.dto;

/**
 * Response returned when a user joins a drop or looks up their queue position,
 * exposing only the user id and assigned sequence number.
 */
public class JoinDropResponse {

    private final String userId;
    private final long sequenceNumber;

    public JoinDropResponse(String userId, long sequenceNumber) {
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
