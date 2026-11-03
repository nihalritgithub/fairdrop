package com.fairdrop.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "drops")
public class Drop {

    @Id
    private String id;

    private String name;

    private int totalInventory;

    @Column(name = "allocated_count", nullable = false)
    private int allocatedCount;

    @Enumerated(EnumType.STRING)
    private DropStatus status;

    protected Drop() {
    }

    public Drop(String id, String name, int totalInventory) {
        this(id, name, totalInventory, 0, DropStatus.OPEN);
    }

    /**
     * Rebuilds a detached drop from a previously captured state, e.g. a cache entry. The result is
     * read-only by convention: it is never used to make allocation decisions.
     */
    public Drop(String id, String name, int totalInventory, int allocatedCount, DropStatus status) {
        this.id = id;
        this.name = name;
        this.totalInventory = totalInventory;
        this.allocatedCount = allocatedCount;
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public boolean hasInventory() {
        return allocatedCount < totalInventory;
    }

    public void allocateOne() {
        if (!hasInventory()) {
            throw new IllegalStateException("Drop is sold out");
        }

        allocatedCount++;
    }

    public int getAllocatedCount() {
        return allocatedCount;
    }

    public String getName() {
        return name;
    }

    public int getTotalInventory() {
        return totalInventory;
    }

    public DropStatus getStatus() {
        return status;
    }

    public void setStatus(DropStatus status) {
        this.status = status;
    }
}
