package com.fairdrop.repository;

import com.fairdrop.model.Drop;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface DropJpaRepository extends JpaRepository<Drop, String> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            SELECT d
            FROM Drop d
            WHERE d.id = :dropId
            """)
    Optional<Drop> findByIdForUpdate(@Param("dropId") String dropId);
}