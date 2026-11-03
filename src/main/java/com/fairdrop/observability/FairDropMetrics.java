package com.fairdrop.observability;

import com.fairdrop.model.AllocationStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;

/**
 * Business and bottleneck metrics for FairDrop. Meter names are defined here so dashboards and
 * alerts have a single place to look; Prometheus exposes them as {@code fairdrop_*}.
 */
@Component
public class FairDropMetrics {

    private final MeterRegistry registry;

    private final Counter joinAttempts;
    private final Counter joinRateLimited;
    private final Counter rateLimiterUnavailable;
    private final Timer rateLimitCheck;
    private final Timer joinDuration;
    private final Timer joinSequenceLockWait;

    private final Timer allocationLockWait;
    private final Map<AllocationStatus, Counter> allocationOutcomes = new EnumMap<>(AllocationStatus.class);
    private final Map<AllocationStatus, Timer> allocationDurations = new EnumMap<>(AllocationStatus.class);

    private final Counter outboxPublished;
    private final Counter outboxPublishFailed;
    private final Counter outboxExhausted;

    public FairDropMetrics(MeterRegistry registry) {
        this.registry = registry;

        joinAttempts = Counter.builder("fairdrop.join.attempts")
                .description("Join requests received, before rate limiting")
                .register(registry);
        joinRateLimited = Counter.builder("fairdrop.join.rate.limited")
                .description("Join requests rejected by the rate limiter (HTTP 429)")
                .register(registry);
        rateLimiterUnavailable = Counter.builder("fairdrop.ratelimit.unavailable")
                .description("Join requests rejected because Redis could not be consulted (HTTP 503)")
                .register(registry);
        rateLimitCheck = Timer.builder("fairdrop.ratelimit.check")
                .description("Latency of the Redis rate-limit script")
                .register(registry);
        joinDuration = Timer.builder("fairdrop.join.duration")
                .description("Latency of persisting or looking up a queue entry")
                .register(registry);
        joinSequenceLockWait = Timer.builder("fairdrop.join.sequence.lock.wait")
                .description("Time spent acquiring the per-drop sequence row lock")
                .register(registry);

        allocationLockWait = Timer.builder("fairdrop.allocation.lock.wait")
                .description("Time spent acquiring the drop row lock for allocation")
                .register(registry);
        for (AllocationStatus status : AllocationStatus.values()) {
            String outcome = status.name().toLowerCase(Locale.ROOT);
            allocationOutcomes.put(status, Counter.builder("fairdrop.allocation.outcomes")
                    .description("Allocate-next attempts by outcome")
                    .tag("outcome", outcome)
                    .register(registry));
            allocationDurations.put(status, Timer.builder("fairdrop.allocation.duration")
                    .description("Latency of allocate-next by outcome")
                    .tag("outcome", outcome)
                    .register(registry));
        }

        outboxPublished = Counter.builder("fairdrop.outbox.publish")
                .description("Outbox publish attempts by result")
                .tag("result", "success")
                .register(registry);
        outboxPublishFailed = Counter.builder("fairdrop.outbox.publish")
                .description("Outbox publish attempts by result")
                .tag("result", "failure")
                .register(registry);
        outboxExhausted = Counter.builder("fairdrop.outbox.exhausted")
                .description("Outbox events that exceeded their publish attempts and were parked as FAILED")
                .register(registry);
    }

    public void joinAttempt() {
        joinAttempts.increment();
    }

    public void joinRateLimited() {
        joinRateLimited.increment();
    }

    public void rateLimiterUnavailable() {
        rateLimiterUnavailable.increment();
    }

    public Timer rateLimitCheck() {
        return rateLimitCheck;
    }

    public Timer joinDuration() {
        return joinDuration;
    }

    public Timer joinSequenceLockWait() {
        return joinSequenceLockWait;
    }

    public Timer allocationLockWait() {
        return allocationLockWait;
    }

    public Timer.Sample startTimer() {
        return Timer.start(registry);
    }

    public void allocationCompleted(Timer.Sample sample, AllocationStatus status) {
        sample.stop(allocationDurations.get(status));
        allocationOutcomes.get(status).increment();
    }

    public void cacheRequest(String cache, String result) {
        registry.counter("fairdrop.cache.requests", "cache", cache, "result", result).increment();
    }

    public void cacheWriteError(String cache, String operation) {
        registry.counter("fairdrop.cache.write.errors", "cache", cache, "operation", operation).increment();
    }

    public void outboxPublished() {
        outboxPublished.increment();
    }

    public void outboxPublishFailed() {
        outboxPublishFailed.increment();
    }

    public void outboxExhausted() {
        outboxExhausted.increment();
    }

    public void eventConsumed(String consumer, String result) {
        registry.counter("fairdrop.events.consumed", "consumer", consumer, "result", result).increment();
    }

    public void eventDeadLettered(String topic) {
        registry.counter("fairdrop.events.dead.lettered", "topic", topic).increment();
    }
}
