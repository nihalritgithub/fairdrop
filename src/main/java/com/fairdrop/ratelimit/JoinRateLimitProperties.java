package com.fairdrop.ratelimit;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Fixed-window limit applied per user and drop to {@code POST /drops/{dropId}/join}.
 *
 * @param maxRequests requests allowed per window
 * @param window      window length; whole seconds are used because Redis expiry is set in seconds
 */
@Validated
@ConfigurationProperties("fairdrop.rate-limit.join")
public record JoinRateLimitProperties(
        @Positive @DefaultValue("5") long maxRequests,
        @NotNull @DurationMin(seconds = 1) @DefaultValue("1m") Duration window) {
}
