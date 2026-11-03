package com.fairdrop.repository.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "queue_entries", uniqueConstraints = {@UniqueConstraint(name = "uk_queue_drop_user", columnNames = {"drop_id", "user_id"}), @UniqueConstraint(name = "uk_queue_drop_sequence", columnNames = {"drop_id", "sequence_number"})})
public class QueueEntryEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    @Column(name = "drop_id", nullable = false)
    private String dropId;

    @Column(name = "user_id", nullable = false)
    private String userId;

    @Column(name = "sequence_number", nullable = false)
    private long sequenceNumber;

    protected QueueEntryEntity() {
    }

    public QueueEntryEntity(String dropId, String userId, long sequenceNumber) {
        this.dropId = dropId;
        this.userId = userId;
        this.sequenceNumber = sequenceNumber;
    }

    public String getId() {
        return id;
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
}