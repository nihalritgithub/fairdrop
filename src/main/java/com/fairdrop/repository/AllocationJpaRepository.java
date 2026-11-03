package com.fairdrop.repository;

import com.fairdrop.repository.entity.AllocationEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AllocationJpaRepository
        extends JpaRepository<AllocationEntity, String> {
}