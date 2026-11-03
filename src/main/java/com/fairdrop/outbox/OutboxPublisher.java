package com.fairdrop.outbox;

import com.fairdrop.messaging.EventHeaders;
import com.fairdrop.observability.CorrelationId;
import com.fairdrop.observability.FairDropMetrics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Relays pending outbox events to Kafka (at-least-once).
 *
 * <p>Each run claims a batch with {@code FOR UPDATE SKIP LOCKED}, sends it, and in the same
 * transaction marks each event PUBLISHED only after Kafka acknowledged it. Failed events are
 * retried with exponential backoff and parked as FAILED after {@code max-attempts}. A crash between
 * the Kafka acknowledgement and the commit re-sends the event, which consumers de-duplicate.
 */
@Component
@ConditionalOnBooleanProperty(name = "fairdrop.outbox.publisher.enabled", matchIfMissing = true)
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventJpaRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TransactionTemplate transactionTemplate;
    private final OutboxProperties.Publisher settings;
    private final Duration retention;
    private final FairDropMetrics metrics;
    private final Clock clock;

    public OutboxPublisher(
            OutboxEventJpaRepository repository,
            KafkaTemplate<String, String> kafkaTemplate,
            TransactionTemplate transactionTemplate,
            OutboxProperties properties,
            FairDropMetrics metrics,
            Clock clock) {

        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.transactionTemplate = transactionTemplate;
        this.settings = properties.publisher();
        this.retention = properties.retention();
        this.metrics = metrics;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${fairdrop.outbox.publisher.poll-interval:1s}")
    public void publishPendingEvents() {
        BatchResult result;
        do {
            result = publishNextBatch();
        } while (result.claimed() == settings.batchSize() && result.failed() == 0);
    }

    /**
     * Claims and relays one batch in its own transaction.
     */
    public BatchResult publishNextBatch() {
        BatchResult result = transactionTemplate.execute(status -> {
            List<OutboxEventEntity> batch = repository.lockPublishableBatch(clock.instant(), settings.batchSize());
            return batch.isEmpty() ? BatchResult.EMPTY : publish(batch);
        });
        return result == null ? BatchResult.EMPTY : result;
    }

    @Scheduled(cron = "${fairdrop.outbox.cleanup-cron:0 30 3 * * *}")
    public void purgePublishedEvents() {
        Instant cutoff = clock.instant().minus(retention);
        Integer deleted = transactionTemplate.execute(status -> repository.deletePublishedBefore(cutoff));
        log.info("Purged published outbox events olderThan={} deleted={}", cutoff, deleted);
    }

    private BatchResult publish(List<OutboxEventEntity> batch) {
        List<InFlight> inFlight = new ArrayList<>(batch.size());

        for (OutboxEventEntity event : batch) {
            CompletableFuture<SendResult<String, String>> future = send(event);
            inFlight.add(new InFlight(event, future));

            // An immediately failed send usually means the broker is unreachable (metadata timeout).
            // Stop here rather than blocking once per remaining event; they stay due for the next run.
            if (future.isCompletedExceptionally()) {
                break;
            }
        }

        long deadline = System.nanoTime() + settings.sendTimeout().toNanos();
        int published = 0;
        int failed = 0;

        for (InFlight send : inFlight) {
            try {
                send.future().get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                send.event().markPublished(clock.instant());
                metrics.outboxPublished();
                published++;
                log.debug("Outbox event published eventId={} topic={}", send.event().getId(), send.event().getTopic());
            } catch (ExecutionException ex) {
                recordFailure(send.event(), ex.getCause());
                failed++;
            } catch (TimeoutException ex) {
                recordFailure(send.event(), ex);
                failed++;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                recordFailure(send.event(), ex);
                failed++;
            }
        }

        return new BatchResult(batch.size(), published, failed);
    }

    private CompletableFuture<SendResult<String, String>> send(OutboxEventEntity event) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(EventHeaders.EVENT_ID, utf8(event.getId().toString()));
        headers.add(EventHeaders.EVENT_TYPE, utf8(event.getEventType()));
        if (event.getCorrelationId() != null) {
            headers.add(CorrelationId.KAFKA_HEADER, utf8(event.getCorrelationId()));
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(
                event.getTopic(), null, event.getMessageKey(), event.getPayload(), headers);
        try {
            return kafkaTemplate.send(record);
        } catch (RuntimeException ex) {
            return CompletableFuture.failedFuture(ex);
        }
    }

    private void recordFailure(OutboxEventEntity event, Throwable cause) {
        String error = String.valueOf(cause);
        Instant retryAt = clock.instant().plus(settings.backoffAfter(event.getAttempts() + 1));
        event.recordFailedAttempt(error, retryAt, settings.maxAttempts());
        metrics.outboxPublishFailed();

        if (event.getStatus() == OutboxStatus.FAILED) {
            metrics.outboxExhausted();
            log.error("Outbox event parked after {} attempts eventId={} topic={} cause={}",
                    event.getAttempts(), event.getId(), event.getTopic(), error);
        } else {
            log.warn("Outbox publish failed eventId={} attempt={} retryAt={} cause={}",
                    event.getId(), event.getAttempts(), retryAt, error);
        }
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public record BatchResult(int claimed, int published, int failed) {

        static final BatchResult EMPTY = new BatchResult(0, 0, 0);
    }

    private record InFlight(OutboxEventEntity event, CompletableFuture<SendResult<String, String>> future) {
    }
}
