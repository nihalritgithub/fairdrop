package com.fairdrop.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes outbox backlog gauges. Values are refreshed on a schedule rather than on scrape so a
 * slow or unavailable database never stalls the metrics endpoint. Runs even when this instance does
 * not relay events, because that is exactly when the backlog grows.
 */
@Component
public class OutboxBacklogMonitor {

    private static final Logger log = LoggerFactory.getLogger(OutboxBacklogMonitor.class);

    private final OutboxEventJpaRepository repository;
    private final Clock clock;
    private final AtomicLong pendingEvents = new AtomicLong();
    private final AtomicLong oldestPendingAgeSeconds = new AtomicLong();

    public OutboxBacklogMonitor(OutboxEventJpaRepository repository, Clock clock, MeterRegistry registry) {
        this.repository = repository;
        this.clock = clock;

        Gauge.builder("fairdrop.outbox.pending", pendingEvents, AtomicLong::get)
                .description("Outbox events waiting to be published")
                .register(registry);
        Gauge.builder("fairdrop.outbox.oldest.pending.age", oldestPendingAgeSeconds, AtomicLong::get)
                .description("Age of the oldest unpublished outbox event")
                .baseUnit("seconds")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${fairdrop.outbox.backlog-refresh-interval:15s}")
    public void refresh() {
        try {
            pendingEvents.set(repository.countByStatus(OutboxStatus.PENDING));
            oldestPendingAgeSeconds.set(repository.findOldestCreatedAt(OutboxStatus.PENDING)
                    .map(createdAt -> Math.max(0, Duration.between(createdAt, clock.instant()).toSeconds()))
                    .orElse(0L));
        } catch (DataAccessException ex) {
            log.warn("Could not refresh outbox backlog metrics cause={}", ex.getMessage());
        }
    }
}
