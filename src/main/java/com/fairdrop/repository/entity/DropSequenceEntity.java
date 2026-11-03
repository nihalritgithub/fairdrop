package com.fairdrop.repository.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "drop_sequences")
public class DropSequenceEntity {

    @Id
    @Column(name = "drop_id")
    private String dropId;

    @Column(name = "last_sequence", nullable = false)
    private long lastSequence;

    protected DropSequenceEntity() {
    }

    public DropSequenceEntity(String dropId) {
        this.dropId = dropId;
        this.lastSequence = 0;
    }

    public String getDropId() {
        return dropId;
    }

    public long getLastSequence() {
        return lastSequence;
    }

    public long nextSequence() {
        return ++lastSequence;
    }
}