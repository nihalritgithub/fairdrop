package com.fairdrop.repository;

import com.fairdrop.repository.entity.QueueEntryEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface QueueEntryJpaRepository extends JpaRepository<QueueEntryEntity, String> {

    Optional<QueueEntryEntity> findByDropIdAndUserId(String dropId, String userId);

    @Query(
            value = """
                    SELECT qe.*
                    FROM queue_entries qe
                    WHERE qe.drop_id = :dropId
                      AND NOT EXISTS (
                          SELECT 1
                          FROM allocations a
                          WHERE a.drop_id = qe.drop_id
                            AND a.user_id = qe.user_id
                      )
                    ORDER BY qe.sequence_number ASC
                    LIMIT 1
                    """,
            nativeQuery = true
    )
    Optional<QueueEntryEntity> findNextUnallocatedUser(@Param("dropId") String dropId);
}
