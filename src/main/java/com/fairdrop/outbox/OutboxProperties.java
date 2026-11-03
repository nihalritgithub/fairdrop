package com.fairdrop.outbox;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Settings for relaying outbox events to Kafka.
 *
 * @param publisher how often and how hard the relay retries
 * @param retention how long published events are kept before cleanup
 */
@Validated
@ConfigurationProperties("fairdrop.outbox")
public record OutboxProperties(
        @Valid @NotNull @DefaultValue Publisher publisher,
        @NotNull @DurationMin(hours = 1) @DefaultValue("7d") Duration retention) {

    /**
     * @param enabled        whether this instance relays events; the outbox is still written when off
     * @param pollInterval   delay between relay runs
     * @param batchSize      events claimed per transaction
     * @param sendTimeout    upper bound on waiting for Kafka acknowledgements of one batch
     * @param maxAttempts    publish attempts before an event is parked as FAILED
     * @param initialBackoff delay after the first failure, doubled on each further failure
     * @param maxBackoff     cap for the exponential backoff
     */
    public record Publisher(
            @DefaultValue("true") boolean enabled,
            @NotNull @DurationMin(millis = 100) @DefaultValue("1s") Duration pollInterval,
            @Positive @Max(1000) @DefaultValue("100") int batchSize,
            @NotNull @DurationMin(seconds = 1) @DefaultValue("30s") Duration sendTimeout,
            @Positive @DefaultValue("20") int maxAttempts,
            @NotNull @DurationMin(millis = 100) @DefaultValue("1s") Duration initialBackoff,
            @NotNull @DurationMin(seconds = 1) @DefaultValue("5m") Duration maxBackoff) {

        public Duration backoffAfter(int failedAttempts) {
            int exponent = Math.min(Math.max(failedAttempts - 1, 0), 30);
            Duration backoff = initialBackoff.multipliedBy(1L << exponent);
            return backoff.compareTo(maxBackoff) > 0 ? maxBackoff : backoff;
        }
    }
}
