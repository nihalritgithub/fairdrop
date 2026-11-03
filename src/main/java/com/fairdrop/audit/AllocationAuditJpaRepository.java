package com.fairdrop.audit;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface AllocationAuditJpaRepository extends JpaRepository<AllocationAuditEntity, UUID> {
}
