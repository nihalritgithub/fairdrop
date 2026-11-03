package com.fairdrop.repository.entity;

import jakarta.persistence.*;

@Entity
@Table(
        name = "allocations",
        uniqueConstraints = {
                @UniqueConstraint(
                        name = "uk_allocation_drop_user",
                        columnNames = {"drop_id", "user_id"}
                )
        }
)
public class AllocationEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    @Column(name = "drop_id", nullable = false)
    private String dropId;

    @Column(name = "user_id", nullable = false)
    private String userId;

    protected AllocationEntity() {
    }

    public AllocationEntity(String dropId, String userId) {
        this.dropId = dropId;
        this.userId = userId;
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
}