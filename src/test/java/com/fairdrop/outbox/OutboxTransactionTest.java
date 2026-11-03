package com.fairdrop.outbox;

import com.fairdrop.event.AllocationCreatedEvent;
import com.fairdrop.model.AllocationResult;
import com.fairdrop.model.AllocationStatus;
import com.fairdrop.model.Drop;
import com.fairdrop.service.DropService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The allocation and its outbox record must share one PostgreSQL transaction.
 */
@SpringBootTest
class OutboxTransactionTest {

    @Autowired
    private DropService dropService;

    @Autowired
    private OutboxEventRecorder outboxEventRecorder;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private JsonMapper jsonMapper;

    @Test
    void successfulAllocationWritesExactlyOnePendingOutboxEvent() {
        Drop drop = dropService.createDrop("Outbox-Test", 1);
        dropService.joinDrop(drop.getId(), "USER-A");

        AllocationResult result = dropService.allocateNext(drop.getId());

        assertThat(result.getStatus()).isEqualTo(AllocationStatus.ALLOCATED);

        List<Map<String, Object>> rows = outboxRows(drop.getId());
        assertThat(rows).hasSize(1);

        Map<String, Object> row = rows.getFirst();
        assertThat(row.get("status")).isEqualTo("PENDING");
        assertThat(row.get("event_type")).isEqualTo(AllocationCreatedEvent.TYPE);
        assertThat(row.get("topic")).isEqualTo("fairdrop.allocation-events");
        assertThat(row.get("message_key")).isEqualTo(drop.getId());

        AllocationCreatedEvent event =
                jsonMapper.readValue((String) row.get("payload"), AllocationCreatedEvent.class);
        assertThat(event.eventId()).isEqualTo(row.get("id"));
        assertThat(event.dropId()).isEqualTo(drop.getId());
        assertThat(event.userId()).isEqualTo("USER-A");
        assertThat(event.sequenceNumber()).isEqualTo(1);
        assertThat(event.allocationId()).isEqualTo(result.getAllocationId().orElseThrow());
    }

    @Test
    void soldOutAndNoEligibleUserOutcomesWriteNoEvents() {
        Drop drop = dropService.createDrop("Outbox-No-Event-Test", 1);

        assertThat(dropService.allocateNext(drop.getId()).getStatus()).isEqualTo(AllocationStatus.NO_ELIGIBLE_USER);

        dropService.joinDrop(drop.getId(), "USER-A");
        dropService.joinDrop(drop.getId(), "USER-B");
        dropService.allocateNext(drop.getId());

        assertThat(dropService.allocateNext(drop.getId()).getStatus()).isEqualTo(AllocationStatus.SOLD_OUT);
        assertThat(outboxRows(drop.getId())).hasSize(1);
    }

    @Test
    void rollbackDiscardsAllocationAndOutboxEventTogether() {
        Drop drop = dropService.createDrop("Outbox-Rollback-Test", 1);
        dropService.joinDrop(drop.getId(), "USER-A");

        transactionTemplate.executeWithoutResult(status -> {
            assertThat(dropService.allocateNext(drop.getId()).getStatus()).isEqualTo(AllocationStatus.ALLOCATED);
            status.setRollbackOnly();
        });

        assertThat(outboxRows(drop.getId())).isEmpty();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM allocations WHERE drop_id = ?", Integer.class, drop.getId())).isZero();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT allocated_count FROM drops WHERE id = ?", Integer.class, drop.getId())).isZero();
    }

    @Test
    void recorderRefusesToWriteOutsideATransaction() {
        AllocationCreatedEvent event =
                AllocationCreatedEvent.of("allocation-1", "drop-1", "user-1", 1, Instant.now());

        assertThatThrownBy(() -> outboxEventRecorder.onAllocationCreated(event))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    private List<Map<String, Object>> outboxRows(String dropId) {
        return jdbcTemplate.queryForList("SELECT * FROM outbox_events WHERE aggregate_id = ?", dropId);
    }
}
