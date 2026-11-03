package com.fairdrop.ratelimit;

import com.fairdrop.exception.RateLimiterUnavailableException;
import com.fairdrop.observability.FairDropMetrics;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Per-user, per-drop fixed-window rate limiter for queue joins, backed by an atomic Redis Lua script.
 *
 * <p>Fails closed: if Redis cannot be consulted the join is rejected with
 * {@link RateLimiterUnavailableException} rather than admitted unprotected.
 */
@Component
public class JoinRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(JoinRateLimiter.class);

    private static final String KEY_PREFIX = "rate:join:";

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<Long> rateLimitScript;
    private final JoinRateLimitProperties properties;
    private final FairDropMetrics metrics;

    public JoinRateLimiter(
            StringRedisTemplate redisTemplate,
            JoinRateLimitProperties properties,
            FairDropMetrics metrics) {

        this.redisTemplate = redisTemplate;
        this.rateLimitScript = RedisScript.of(new ClassPathResource("scripts/rate-limit.lua"), Long.class);
        this.properties = properties;
        this.metrics = metrics;
    }

    /**
     * Counts this join attempt and reports whether it is within the configured limit.
     *
     * @throws RateLimiterUnavailableException if Redis fails or times out
     */
    public boolean allow(String dropId, String userId) {
        metrics.joinAttempt();

        Long requestCount;
        Timer.Sample sample = metrics.startTimer();
        try {
            requestCount = redisTemplate.execute(
                    rateLimitScript,
                    List.of(KEY_PREFIX + dropId + ":" + userId),
                    String.valueOf(properties.window().toSeconds()));
        } catch (DataAccessException ex) {
            metrics.rateLimiterUnavailable();
            log.warn("Join rate limiter unavailable, rejecting join dropId={} cause={}", dropId, ex.getMessage());
            throw new RateLimiterUnavailableException(ex);
        } finally {
            sample.stop(metrics.rateLimitCheck());
        }

        if (requestCount == null) {
            metrics.rateLimiterUnavailable();
            throw new RateLimiterUnavailableException(null);
        }

        boolean allowed = requestCount <= properties.maxRequests();
        if (!allowed) {
            metrics.joinRateLimited();
            log.debug("Join rate limit exceeded dropId={} userId={} count={}", dropId, userId, requestCount);
        }
        return allowed;
    }
}
