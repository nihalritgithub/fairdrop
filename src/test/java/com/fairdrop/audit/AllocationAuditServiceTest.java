package com.fairdrop.audit;

import com.fairdrop.event.AllocationCreatedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Duplicate deliveries of the same event must not duplicate the audit side effect.
 */
@SpringBootTest
class AllocationAuditServiceTest {

    @Autowired
    private AllocationAuditService auditService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void redeliveredEventIsRecordedOnce() {
        AllocationCreatedEvent event = newEvent();

        assertThat(auditService.record(event)).isTrue();
        assertThat(auditService.record(event)).isFalse();

        assertThat(auditRows(event)).isEqualTo(1);
        assertThat(processedMarkers(event)).isEqualTo(1);
    }

    @Test
    void concurrentDuplicateDeliveriesApplyTheSideEffectOnce() throws Exception {
        AllocationCreatedEvent event = newEvent();
        int deliveries = 10;

        ExecutorService executor = Executors.newFixedThreadPool(deliveries);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < deliveries; i++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    return auditService.record(event);
                }));
            }
            start.countDown();

            int applied = 0;
            for (Future<Boolean> future : futures) {
                if (future.get()) {
                    applied++;
                }
            }

            assertThat(applied).isEqualTo(1);
            assertThat(auditRows(event)).isEqualTo(1);
            assertThat(processedMarkers(event)).isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void auditRowCarriesTheEventData() {
        AllocationCreatedEvent event = newEvent();

        auditService.record(event);

        var row = jdbcTemplate.queryForMap("SELECT * FROM allocation_audit_log WHERE event_id = ?", event.eventId());
        assertThat(row.get("allocation_id")).isEqualTo(event.allocationId());
        assertThat(row.get("drop_id")).isEqualTo(event.dropId());
        assertThat(row.get("user_id")).isEqualTo(event.userId());
        assertThat(row.get("sequence_number")).isEqualTo(event.sequenceNumber());
    }

    private static AllocationCreatedEvent newEvent() {
        return AllocationCreatedEvent.of(
                "allocation-" + UUID.randomUUID(),
                "drop-" + UUID.randomUUID(),
                "user-" + UUID.randomUUID(),
                3,
                Instant.now().truncatedTo(ChronoUnit.MICROS));
    }

    private int auditRows(AllocationCreatedEvent event) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM allocation_audit_log WHERE event_id = ?", Integer.class, event.eventId());
    }

    private int processedMarkers(AllocationCreatedEvent event) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_events WHERE consumer_name = ? AND event_id = ?",
                Integer.class,
                AllocationAuditService.CONSUMER_NAME,
                event.eventId());
    }
}
