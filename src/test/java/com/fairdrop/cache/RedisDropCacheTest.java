package com.fairdrop.cache;

import com.fairdrop.model.Drop;
import com.fairdrop.observability.FairDropMetrics;
import com.fairdrop.repository.InMemoryFairDropRepository;
import com.fairdrop.service.DropService;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that the drop cache fails open: Redis errors degrade to cache misses, and a tripped
 * circuit breaker stops calling Redis altogether.
 */
class RedisDropCacheTest {

    private final StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);

    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> valueOperations = mock(ValueOperations.class);

    private SimpleMeterRegistry meterRegistry;
    private RedisDropCache cache;

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);

        meterRegistry = new SimpleMeterRegistry();
        DropCacheProperties properties = new DropCacheProperties(
                true,
                Duration.ofSeconds(30),
                new DropCacheProperties.CircuitBreaker(50, 4, 4, Duration.ofMinutes(1)));

        cache = new RedisDropCache(
                redisTemplate,
                JsonMapper.builder().build(),
                properties,
                new FairDropMetrics(meterRegistry),
                CircuitBreakerRegistry.ofDefaults());
    }

    @Test
    void readFailureIsTreatedAsMiss() {
        when(valueOperations.get(anyString())).thenThrow(new RedisConnectionFailureException("down"));

        assertThat(cache.find("drop-1")).isEmpty();
        assertThat(cacheRequests("error")).isEqualTo(1.0);
    }

    @Test
    void dropServiceStillServesReadsFromDatabaseWhenRedisIsDown() {
        when(valueOperations.get(anyString())).thenThrow(new RedisConnectionFailureException("down"));
        doThrow(new RedisConnectionFailureException("down"))
                .when(valueOperations).set(anyString(), anyString(), any(Duration.class));

        DropService dropService = new DropService(
                new InMemoryFairDropRepository(),
                cache,
                event -> {
                },
                new FairDropMetrics(meterRegistry));
        Drop created = dropService.createDrop("Redis-Down", 3);

        Drop loaded = dropService.getDrop(created.getId());

        assertThat(loaded.getId()).isEqualTo(created.getId());
        assertThat(loaded.getTotalInventory()).isEqualTo(3);
    }

    @Test
    void openCircuitStopsCallingRedis() {
        when(valueOperations.get(anyString())).thenThrow(new QueryTimeoutException("slow"));

        for (int i = 0; i < 4; i++) {
            cache.find("drop-1");
        }
        for (int i = 0; i < 10; i++) {
            assertThat(cache.find("drop-1")).isEmpty();
        }

        verify(valueOperations, times(4)).get(anyString());
        assertThat(cacheRequests("bypassed")).isEqualTo(10.0);
    }

    @Test
    void unreadableEntryIsEvictedAndTreatedAsMiss() {
        when(valueOperations.get(anyString())).thenReturn("{not-json");

        assertThat(cache.find("drop-1")).isEmpty();
        verify(redisTemplate).delete(RedisDropCache.key("drop-1"));
    }

    @Test
    void evictionFailureIsSwallowed() {
        when(redisTemplate.delete(anyString())).thenThrow(new RedisConnectionFailureException("down"));

        cache.evict("drop-1");

        assertThat(meterRegistry.get("fairdrop.cache.write.errors").tag("operation", "evict").counter().count())
                .isEqualTo(1.0);
    }

    private double cacheRequests(String result) {
        return meterRegistry.get("fairdrop.cache.requests").tag("result", result).counter().count();
    }
}
