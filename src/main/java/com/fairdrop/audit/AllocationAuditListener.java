package com.fairdrop.audit;

import com.fairdrop.event.AllocationCreatedEvent;
import com.fairdrop.messaging.MalformedEventException;
import com.fairdrop.observability.CorrelationId;
import com.fairdrop.observability.FairDropMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import static com.fairdrop.messaging.EventHeaders.lastValue;

/**
 * Consumes allocation events from Kafka into the audit log. Offsets are committed only after the
 * handler returns, so processing is at-least-once and relies on {@link AllocationAuditService}
 * for de-duplication. Failures are retried and then dead-lettered by the container error handler.
 */
@Component
public class AllocationAuditListener {

    private final AllocationAuditService auditService;
    private final JsonMapper jsonMapper;
    private final FairDropMetrics metrics;

    public AllocationAuditListener(
            AllocationAuditService auditService,
            JsonMapper jsonMapper,
            FairDropMetrics metrics) {

        this.auditService = auditService;
        this.jsonMapper = jsonMapper;
        this.metrics = metrics;
    }

    @KafkaListener(
            id = AllocationAuditService.CONSUMER_NAME,
            topics = "#{@allocationEventsTopic.name()}",
            groupId = "${fairdrop.kafka.allocation-audit-group-id:fairdrop-allocation-audit}")
    public void onAllocationCreated(ConsumerRecord<String, String> record) {
        String correlationId = CorrelationId.acceptOrGenerate(
                lastValue(record.headers(), CorrelationId.KAFKA_HEADER).orElse(null));

        MDC.put(CorrelationId.MDC_KEY, correlationId);
        try {
            boolean recorded = auditService.record(parse(record.value()));
            metrics.eventConsumed(AllocationAuditService.CONSUMER_NAME, recorded ? "processed" : "duplicate");
        } catch (RuntimeException ex) {
            metrics.eventConsumed(AllocationAuditService.CONSUMER_NAME, "failed");
            throw ex;
        } finally {
            MDC.remove(CorrelationId.MDC_KEY);
        }
    }

    private AllocationCreatedEvent parse(String payload) {
        AllocationCreatedEvent event;
        try {
            event = jsonMapper.readValue(payload, AllocationCreatedEvent.class);
        } catch (JacksonException ex) {
            throw new MalformedEventException("Unreadable AllocationCreated payload", ex);
        }

        if (event == null
                || event.eventId() == null
                || event.allocationId() == null
                || event.dropId() == null
                || event.userId() == null
                || event.occurredAt() == null) {
            throw new MalformedEventException("AllocationCreated payload is missing required fields", null);
        }
        return event;
    }
}
