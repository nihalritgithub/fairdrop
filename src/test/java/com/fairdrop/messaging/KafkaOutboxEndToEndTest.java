package com.fairdrop.messaging;

import com.fairdrop.audit.AllocationAuditJpaRepository;
import com.fairdrop.model.AllocationStatus;
import com.fairdrop.model.Drop;
import com.fairdrop.service.DropService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Full asynchronous path on an embedded Kafka broker: allocation, outbox row, relay, consumer,
 * audit row; plus duplicate delivery and dead-lettering of poison messages.
 */
@SpringBootTest(properties = {
        "fairdrop.outbox.publisher.enabled=true",
        "fairdrop.outbox.publisher.poll-interval=200ms",
        "spring.kafka.listener.auto-startup=true",
        "spring.kafka.admin.auto-create=true",
        "fairdrop.kafka.topic-partitions=1",
        "fairdrop.kafka.consumer-retry.max-retries=1",
        "fairdrop.kafka.consumer-retry.initial-interval=50ms"
})
@EmbeddedKafka(partitions = 1, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@DirtiesContext
class KafkaOutboxEndToEndTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    @Autowired
    private DropService dropService;

    @Autowired
    private AllocationAuditJpaRepository auditRepository;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private FairDropKafkaProperties kafkaProperties;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Test
    void allocationIsRelayedThroughKafkaIntoTheAuditLog() {
        Drop drop = dropService.createDrop("Kafka-E2E-Test", 1);
        dropService.joinDrop(drop.getId(), "USER-A");

        assertThat(dropService.allocateNext(drop.getId()).getStatus()).isEqualTo(AllocationStatus.ALLOCATED);

        UUID eventId = jdbcTemplate.queryForObject(
                "SELECT id FROM outbox_events WHERE aggregate_id = ?", UUID.class, drop.getId());

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(outboxStatus(eventId)).isEqualTo("PUBLISHED");
            assertThat(auditRepository.findById(eventId)).hasValueSatisfying(audit -> {
                assertThat(audit.getDropId()).isEqualTo(drop.getId());
                assertThat(audit.getUserId()).isEqualTo("USER-A");
                assertThat(audit.getSequenceNumber()).isEqualTo(1);
            });
        });
    }

    @Test
    void redeliveredEventDoesNotDuplicateTheAuditEntry() throws Exception {
        Drop drop = dropService.createDrop("Kafka-Duplicate-Test", 1);
        dropService.joinDrop(drop.getId(), "USER-A");
        dropService.allocateNext(drop.getId());

        Map<String, Object> outbox = jdbcTemplate.queryForMap(
                "SELECT id, payload FROM outbox_events WHERE aggregate_id = ?", drop.getId());
        UUID eventId = (UUID) outbox.get("id");

        await().atMost(TIMEOUT).until(() -> auditRepository.existsById(eventId));
        double duplicatesBefore = consumed("duplicate");

        kafkaTemplate.send(kafkaProperties.allocationEventsTopic(), drop.getId(), (String) outbox.get("payload"))
                .get();

        await().atMost(TIMEOUT).until(() -> consumed("duplicate") > duplicatesBefore);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM allocation_audit_log WHERE drop_id = ?", Integer.class, drop.getId()))
                .isEqualTo(1);
    }

    @Test
    void poisonMessageIsDeadLetteredAndDoesNotBlockLaterEvents() throws Exception {
        String poison = "not-an-event-" + UUID.randomUUID();
        String deadLetterTopic = kafkaProperties.deadLetterTopic(kafkaProperties.allocationEventsTopic());

        try (Consumer<String, String> consumer = deadLetterConsumer()) {
            consumer.subscribe(List.of(deadLetterTopic));

            kafkaTemplate.send(kafkaProperties.allocationEventsTopic(), "poison", poison).get();

            List<String> deadLettered = new ArrayList<>();
            await().atMost(TIMEOUT).untilAsserted(() -> {
                for (ConsumerRecord<String, String> record : KafkaTestUtils.getRecords(consumer, Duration.ofMillis(500))) {
                    deadLettered.add(record.value());
                }
                assertThat(deadLettered).contains(poison);
            });
        }

        Drop drop = dropService.createDrop("Kafka-After-Poison-Test", 1);
        dropService.joinDrop(drop.getId(), "USER-A");
        dropService.allocateNext(drop.getId());

        await().atMost(TIMEOUT).until(() -> jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM allocation_audit_log WHERE drop_id = ?", Integer.class, drop.getId()) == 1);
    }

    private Consumer<String, String> deadLetterConsumer() {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-verifier-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), new StringDeserializer())
                .createConsumer();
    }

    private String outboxStatus(UUID eventId) {
        return jdbcTemplate.queryForObject("SELECT status FROM outbox_events WHERE id = ?", String.class, eventId);
    }

    private double consumed(String result) {
        Counter counter = meterRegistry.find("fairdrop.events.consumed")
                .tag("consumer", "allocation-audit")
                .tag("result", result)
                .counter();
        return counter == null ? 0 : counter.count();
    }
}
