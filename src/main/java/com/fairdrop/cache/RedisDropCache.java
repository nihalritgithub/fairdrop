package com.fairdrop.cache;

import com.fairdrop.event.AllocationCreatedEvent;
import com.fairdrop.model.Drop;
import com.fairdrop.model.DropStatus;
import com.fairdrop.observability.FairDropMetrics;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionalEventListener;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.util.Optional;

/**
 * Cache-aside store for drops in Redis under {@code fairdrop:cache:drop:<dropId>}.
 *
 * <p>Every Redis problem degrades to a cache miss so reads keep working from PostgreSQL. Reads and
 * writes go through a circuit breaker; evictions do not, because skipping an eviction would leave a
 * stale entry behind once Redis recovers.
 */
@Component
public class RedisDropCache implements DropCache {

    static final String KEY_PREFIX = "fairdrop:cache:drop:";

    private static final Logger log = LoggerFactory.getLogger(RedisDropCache.class);
    private static final String CACHE_NAME = "drop";

    private final StringRedisTemplate redisTemplate;
    private final JsonMapper jsonMapper;
    private final DropCacheProperties properties;
    private final FairDropMetrics metrics;
    private final CircuitBreaker circuitBreaker;

    public RedisDropCache(
            StringRedisTemplate redisTemplate,
            JsonMapper jsonMapper,
            DropCacheProperties properties,
            FairDropMetrics metrics,
            CircuitBreakerRegistry circuitBreakerRegistry) {

        this.redisTemplate = redisTemplate;
        this.jsonMapper = jsonMapper;
        this.properties = properties;
        this.metrics = metrics;
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("dropCache", circuitBreakerConfig(properties));
    }

    @Override
    public Optional<Drop> find(String dropId) {
        if (!properties.enabled()) {
            return Optional.empty();
        }

        String json;
        try {
            json = circuitBreaker.executeSupplier(() -> redisTemplate.opsForValue().get(key(dropId)));
        } catch (CallNotPermittedException ex) {
            metrics.cacheRequest(CACHE_NAME, "bypassed");
            return Optional.empty();
        } catch (DataAccessException ex) {
            metrics.cacheRequest(CACHE_NAME, "error");
            log.warn("Drop cache read failed, falling back to database dropId={} cause={}", dropId, ex.getMessage());
            return Optional.empty();
        }

        if (json == null) {
            metrics.cacheRequest(CACHE_NAME, "miss");
            return Optional.empty();
        }

        try {
            Drop drop = jsonMapper.readValue(json, CachedDrop.class).toDrop();
            metrics.cacheRequest(CACHE_NAME, "hit");
            return Optional.of(drop);
        } catch (JacksonException ex) {
            metrics.cacheRequest(CACHE_NAME, "miss");
            log.warn("Discarding unreadable drop cache entry dropId={}", dropId);
            evict(dropId);
            return Optional.empty();
        }
    }

    @Override
    public void put(Drop drop) {
        if (!properties.enabled()) {
            return;
        }

        String json = jsonMapper.writeValueAsString(CachedDrop.from(drop));
        try {
            circuitBreaker.executeRunnable(
                    () -> redisTemplate.opsForValue().set(key(drop.getId()), json, properties.ttl()));
        } catch (CallNotPermittedException ex) {
            log.debug("Drop cache circuit open, not caching dropId={}", drop.getId());
        } catch (DataAccessException ex) {
            metrics.cacheWriteError(CACHE_NAME, "put");
            log.warn("Drop cache write failed dropId={} cause={}", drop.getId(), ex.getMessage());
        }
    }

    @Override
    public void evict(String dropId) {
        if (!properties.enabled()) {
            return;
        }

        try {
            redisTemplate.delete(key(dropId));
        } catch (DataAccessException ex) {
            metrics.cacheWriteError(CACHE_NAME, "evict");
            log.warn("Drop cache eviction failed, entry expires within {} dropId={} cause={}",
                    properties.ttl(), dropId, ex.getMessage());
        }
    }

    /**
     * Evicts only after the allocation commits; evicting earlier would let a concurrent reader
     * re-cache the pre-allocation state.
     */
    @TransactionalEventListener(fallbackExecution = true)
    public void onAllocationCreated(AllocationCreatedEvent event) {
        evict(event.dropId());
    }

    static String key(String dropId) {
        return KEY_PREFIX + dropId;
    }

    private static CircuitBreakerConfig circuitBreakerConfig(DropCacheProperties properties) {
        DropCacheProperties.CircuitBreaker settings = properties.circuitBreaker();
        return CircuitBreakerConfig.custom()
                .failureRateThreshold(settings.failureRateThreshold())
                .slidingWindowSize(settings.slidingWindowSize())
                .minimumNumberOfCalls(settings.minimumNumberOfCalls())
                .waitDurationInOpenState(settings.waitDurationInOpenState())
                .recordExceptions(DataAccessException.class)
                .build();
    }

    record CachedDrop(String id, String name, int totalInventory, int allocatedCount, DropStatus status) {

        static CachedDrop from(Drop drop) {
            return new CachedDrop(
                    drop.getId(),
                    drop.getName(),
                    drop.getTotalInventory(),
                    drop.getAllocatedCount(),
                    drop.getStatus());
        }

        Drop toDrop() {
            return new Drop(id, name, totalInventory, allocatedCount, status);
        }
    }
}
