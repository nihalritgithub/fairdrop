package com.fairdrop.ratelimit;

import com.fairdrop.exception.RateLimiterUnavailableException;
import com.fairdrop.observability.FairDropMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Duration;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The limiter fails closed for every kind of Redis failure, not only refused connections.
 */
class JoinRateLimiterUnavailableTest {

    static Stream<DataAccessException> redisFailures() {
        return Stream.of(
                new RedisConnectionFailureException("connection refused"),
                new QueryTimeoutException("command timed out"),
                new RedisSystemException("Currently not connected. Commands are rejected.", null));
    }

    @ParameterizedTest
    @MethodSource("redisFailures")
    @SuppressWarnings("unchecked")
    void redisFailureRejectsJoinAsUnavailable(DataAccessException failure) {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        when(redisTemplate.execute(any(RedisScript.class), anyList(), anyString())).thenThrow(failure);

        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        JoinRateLimiter limiter = new JoinRateLimiter(
                redisTemplate,
                new JoinRateLimitProperties(5, Duration.ofMinutes(1)),
                new FairDropMetrics(meterRegistry));

        assertThatThrownBy(() -> limiter.allow("drop-1", "user-1"))
                .isInstanceOf(RateLimiterUnavailableException.class)
                .hasCause(failure);

        assertThat(meterRegistry.get("fairdrop.ratelimit.unavailable").counter().count()).isEqualTo(1.0);
    }
}
