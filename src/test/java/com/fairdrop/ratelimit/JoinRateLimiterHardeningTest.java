package com.fairdrop.ratelimit;

import com.fairdrop.observability.FairDropMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Behavior of the Lua-based limiter beyond the defaults covered by {@link JoinRateLimiterTest}.
 */
@SpringBootTest
class JoinRateLimiterHardeningTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    void limitAndWindowComeFromConfiguration() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        JoinRateLimiter limiter = new JoinRateLimiter(
                redisTemplate,
                new JoinRateLimitProperties(2, Duration.ofSeconds(10)),
                new FairDropMetrics(meterRegistry));

        String dropId = "drop-" + UUID.randomUUID();
        String userId = "user-" + UUID.randomUUID();
        String key = "rate:join:" + dropId + ":" + userId;

        try {
            assertThat(limiter.allow(dropId, userId)).isTrue();
            assertThat(limiter.allow(dropId, userId)).isTrue();
            assertThat(limiter.allow(dropId, userId)).isFalse();

            assertThat(redisTemplate.getExpire(key, TimeUnit.SECONDS)).isPositive().isLessThanOrEqualTo(10);
            assertThat(meterRegistry.get("fairdrop.join.attempts").counter().count()).isEqualTo(3.0);
            assertThat(meterRegistry.get("fairdrop.join.rate.limited").counter().count()).isEqualTo(1.0);
        } finally {
            redisTemplate.delete(key);
        }
    }

    @Test
    void counterWithoutExpiryIsRepairedSoUsersAreNeverLockedOutForever() {
        JoinRateLimiter limiter = new JoinRateLimiter(
                redisTemplate,
                new JoinRateLimitProperties(5, Duration.ofMinutes(1)),
                new FairDropMetrics(new SimpleMeterRegistry()));

        String dropId = "drop-" + UUID.randomUUID();
        String userId = "user-" + UUID.randomUUID();
        String key = "rate:join:" + dropId + ":" + userId;

        try {
            redisTemplate.opsForValue().set(key, "99");
            assertThat(redisTemplate.getExpire(key, TimeUnit.SECONDS)).isEqualTo(-1L);

            assertThat(limiter.allow(dropId, userId)).isFalse();

            assertThat(redisTemplate.getExpire(key, TimeUnit.SECONDS)).isPositive().isLessThanOrEqualTo(60);
        } finally {
            redisTemplate.delete(key);
        }
    }
}
