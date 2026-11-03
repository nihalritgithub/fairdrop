package com.fairdrop.dto;

import com.fairdrop.model.AllocationStatus;

public class AllocationResponse {

    private final AllocationStatus status;
    private final String userId;
    private final Long sequenceNumber;

    public AllocationResponse(
            AllocationStatus status,
            String userId,
            Long sequenceNumber) {

        this.status = status;
        this.userId = userId;
        this.sequenceNumber = sequenceNumber;
    }

    public AllocationStatus getStatus() {
        return status;
    }

    public String getUserId() {
        return userId;
    }

    public Long getSequenceNumber() {
        return sequenceNumber;
    }
}