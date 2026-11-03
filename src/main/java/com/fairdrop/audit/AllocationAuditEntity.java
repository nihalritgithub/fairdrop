package com.fairdrop.audit;

import com.fairdrop.event.AllocationCreatedEvent;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

/**
 * Append-only audit trail of allocations, built asynchronously from {@link AllocationCreatedEvent}s.
 */
@Entity
@Table(name = "allocation_audit_log")
public class AllocationAuditEntity implements Persistable<UUID> {

    @Id
    @Column(name = "event_id")
    private UUID eventId;

    @Column(name = "allocation_id", nullable = false)
    private String allocationId;

    @Column(name = "drop_id", nullable = false)
    private String dropId;

    @Column(name = "user_id", nullable = false)
    private String userId;

    @Column(name = "sequence_number", nullable = false)
    private long sequenceNumber;

    @Column(name = "allocated_at", nullable = false)
    private Instant allocatedAt;

    @Column(name = "recorded_at", nullable = false)
    private Instant recordedAt;

    @Transient
    private boolean newEntity;

    protected AllocationAuditEntity() {
    }

    public AllocationAuditEntity(AllocationCreatedEvent event, Instant recordedAt) {
        this.eventId = event.eventId();
        this.allocationId = event.allocationId();
        this.dropId = event.dropId();
        this.userId = event.userId();
        this.sequenceNumber = event.sequenceNumber();
        this.allocatedAt = event.occurredAt();
        this.recordedAt = recordedAt;
        this.newEntity = true;
    }

    @PostLoad
    @PostPersist
    void markNotNew() {
        this.newEntity = false;
    }

    @Override
    public UUID getId() {
        return eventId;
    }

    @Override
    public boolean isNew() {
        return newEntity;
    }

    public String getAllocationId() {
        return allocationId;
    }

    public String getDropId() {
        return dropId;
    }

    public String getUserId() {
        return userId;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public Instant getAllocatedAt() {
        return allocatedAt;
    }

    public Instant getRecordedAt() {
        return recordedAt;
    }
}
