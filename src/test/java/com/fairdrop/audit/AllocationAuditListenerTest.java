package com.fairdrop.audit;

import com.fairdrop.event.AllocationCreatedEvent;
import com.fairdrop.messaging.MalformedEventException;
import com.fairdrop.observability.FairDropMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Message handling at the Kafka boundary: poison payloads are rejected as non-retryable.
 */
class AllocationAuditListenerTest {

    private final AllocationAuditService auditService = mock(AllocationAuditService.class);
    private final JsonMapper jsonMapper = JsonMapper.builder().build();
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final AllocationAuditListener listener =
            new AllocationAuditListener(auditService, jsonMapper, new FairDropMetrics(meterRegistry));

    @ParameterizedTest
    @ValueSource(strings = {"not json", "null", "{\"dropId\":\"drop-1\"}"})
    void malformedPayloadIsRejectedWithoutSideEffects(String payload) {
        assertThatThrownBy(() -> listener.onAllocationCreated(record(payload)))
                .isInstanceOf(MalformedEventException.class);

        verifyNoInteractions(auditService);
        assertThat(consumed("failed")).isEqualTo(1.0);
    }

    @Test
    void validPayloadIsHandedToTheIdempotentService() {
        AllocationCreatedEvent event = AllocationCreatedEvent.of("a-1", "drop-1", "user-1", 1, Instant.now());
        when(auditService.record(any())).thenReturn(true, false);

        listener.onAllocationCreated(record(jsonMapper.writeValueAsString(event)));
        listener.onAllocationCreated(record(jsonMapper.writeValueAsString(event)));

        assertThat(consumed("processed")).isEqualTo(1.0);
        assertThat(consumed("duplicate")).isEqualTo(1.0);
    }

    private static ConsumerRecord<String, String> record(String payload) {
        return new ConsumerRecord<>("fairdrop.allocation-events", 0, 0L, "drop-1", payload);
    }

    private double consumed(String result) {
        return meterRegistry.get("fairdrop.events.consumed").tag("result", result).counter().count();
    }
}
