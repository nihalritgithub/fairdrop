package com.fairdrop.outbox;

import com.fairdrop.messaging.EventHeaders;
import com.fairdrop.observability.CorrelationId;
import com.fairdrop.observability.FairDropMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * State transitions of outbox events as the relay talks to Kafka.
 */
class OutboxPublisherTest {

    private static final Instant NOW = Instant.parse("2026-01-01T10:00:00Z");

    private final OutboxEventJpaRepository repository = mock(OutboxEventJpaRepository.class);

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);

    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
    }

    @Test
    void acknowledgedEventIsMarkedPublishedWithHeaders() {
        OutboxEventEntity event = pendingEvent("corr-1");
        when(repository.lockPublishableBatch(any(), anyInt())).thenReturn(List.of(event));
        when(kafkaTemplate.send(anyRecord())).thenReturn(acknowledged());

        OutboxPublisher.BatchResult result = publisher(20).publishNextBatch();

        assertThat(result.published()).isEqualTo(1);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(event.getPublishedAt()).isEqualTo(NOW);
        assertThat(meterRegistry.get("fairdrop.outbox.publish").tag("result", "success").counter().count())
                .isEqualTo(1.0);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(sent.capture());
        ProducerRecord<String, String> record = sent.getValue();
        assertThat(record.topic()).isEqualTo("fairdrop.allocation-events");
        assertThat(record.key()).isEqualTo("drop-1");
        assertThat(header(record, EventHeaders.EVENT_ID)).isEqualTo(event.getId().toString());
        assertThat(header(record, EventHeaders.EVENT_TYPE)).isEqualTo("AllocationCreated");
        assertThat(header(record, CorrelationId.KAFKA_HEADER)).isEqualTo("corr-1");
    }

    @Test
    void failedSendStaysPendingWithBackoff() {
        OutboxEventEntity event = pendingEvent(null);
        when(repository.lockPublishableBatch(any(), anyInt())).thenReturn(List.of(event));
        when(kafkaTemplate.send(anyRecord()))
                .thenReturn(CompletableFuture.failedFuture(new KafkaException("broker down")));

        OutboxPublisher.BatchResult result = publisher(20).publishNextBatch();

        assertThat(result.failed()).isEqualTo(1);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(event.getAttempts()).isEqualTo(1);
        assertThat(event.getNextAttemptAt()).isEqualTo(NOW.plusSeconds(1));
        assertThat(event.getLastError()).contains("broker down");
        assertThat(meterRegistry.get("fairdrop.outbox.publish").tag("result", "failure").counter().count())
                .isEqualTo(1.0);
    }

    @Test
    void eventIsParkedAfterMaxAttempts() {
        OutboxEventEntity event = pendingEvent(null);
        when(repository.lockPublishableBatch(any(), anyInt())).thenReturn(List.of(event));
        when(kafkaTemplate.send(anyRecord()))
                .thenReturn(CompletableFuture.failedFuture(new KafkaException("rejected")));

        OutboxPublisher publisher = publisher(2);
        publisher.publishNextBatch();
        publisher.publishNextBatch();

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(event.getAttempts()).isEqualTo(2);
        assertThat(meterRegistry.get("fairdrop.outbox.exhausted").counter().count()).isEqualTo(1.0);
    }

    @Test
    void unreachableBrokerStopsDispatchingTheRestOfTheBatch() {
        OutboxEventEntity first = pendingEvent(null);
        OutboxEventEntity second = pendingEvent(null);
        when(repository.lockPublishableBatch(any(), anyInt())).thenReturn(List.of(first, second));
        when(kafkaTemplate.send(anyRecord()))
                .thenReturn(CompletableFuture.failedFuture(new KafkaException("metadata timeout")));

        publisher(20).publishNextBatch();

        verify(kafkaTemplate, times(1)).send(anyRecord());
        assertThat(first.getAttempts()).isEqualTo(1);
        assertThat(second.getAttempts()).isZero();
        assertThat(second.getStatus()).isEqualTo(OutboxStatus.PENDING);
    }

    @Test
    void backoffGrowsExponentiallyUpToTheCap() {
        OutboxProperties.Publisher settings = settings(20);

        assertThat(settings.backoffAfter(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(settings.backoffAfter(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(settings.backoffAfter(5)).isEqualTo(Duration.ofSeconds(16));
        assertThat(settings.backoffAfter(20)).isEqualTo(Duration.ofMinutes(5));
    }

    private OutboxPublisher publisher(int maxAttempts) {
        return new OutboxPublisher(
                repository,
                kafkaTemplate,
                new TransactionTemplate(mock(PlatformTransactionManager.class)),
                new OutboxProperties(settings(maxAttempts), Duration.ofDays(7)),
                new FairDropMetrics(meterRegistry),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private static OutboxProperties.Publisher settings(int maxAttempts) {
        return new OutboxProperties.Publisher(
                true,
                Duration.ofSeconds(1),
                100,
                Duration.ofSeconds(5),
                maxAttempts,
                Duration.ofSeconds(1),
                Duration.ofMinutes(5));
    }

    private static OutboxEventEntity pendingEvent(String correlationId) {
        return OutboxEventEntity.pending(
                UUID.randomUUID(),
                "Drop",
                "drop-1",
                "AllocationCreated",
                "fairdrop.allocation-events",
                "drop-1",
                "{}",
                correlationId,
                NOW.minusSeconds(5));
    }

    private static CompletableFuture<SendResult<String, String>> acknowledged() {
        return CompletableFuture.completedFuture(new SendResult<>(null, null));
    }

    private static ProducerRecord<String, String> anyRecord() {
        return any();
    }

    private static String header(ProducerRecord<String, String> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
