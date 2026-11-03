package com.fairdrop.repository;

import com.fairdrop.repository.entity.DropSequenceEntity;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface DropSequenceJpaRepository extends JpaRepository<DropSequenceEntity, String> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            SELECT sequence
            FROM DropSequenceEntity sequence
            WHERE sequence.dropId = :dropId
            """)
    Optional<DropSequenceEntity> findByDropIdForUpdate(@Param("dropId") String dropId);
}