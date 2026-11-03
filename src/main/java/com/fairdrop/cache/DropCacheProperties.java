package com.fairdrop.cache;

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
 * Settings for the Redis drop cache.
 *
 * @param enabled        whether reads consult Redis at all
 * @param ttl            expiry of a cached drop; bounds staleness if an eviction is ever missed
 * @param circuitBreaker stops calling Redis for a while after repeated failures, so a sick Redis
 *                       does not add its timeout to every read
 */
@Validated
@ConfigurationProperties("fairdrop.cache.drop")
public record DropCacheProperties(
        @DefaultValue("true") boolean enabled,
        @NotNull @DurationMin(seconds = 1) @DefaultValue("30s") Duration ttl,
        @Valid @NotNull @DefaultValue CircuitBreaker circuitBreaker) {

    public record CircuitBreaker(
            @Positive @Max(100) @DefaultValue("50") int failureRateThreshold,
            @Positive @DefaultValue("20") int slidingWindowSize,
            @Positive @DefaultValue("10") int minimumNumberOfCalls,
            @NotNull @DurationMin(millis = 100) @DefaultValue("30s") Duration waitDurationInOpenState) {
    }
}
