package com.fairdrop.cache;

import com.fairdrop.model.AllocationStatus;
import com.fairdrop.model.Drop;
import com.fairdrop.service.DropService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cache-aside behavior against the real Redis and PostgreSQL used by the application.
 */
@SpringBootTest
class DropCacheIntegrationTest {

    @Autowired
    private DropService dropService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    void missLoadsFromDatabaseAndPopulatesRedisWithTtl() {
        Drop drop = dropService.createDrop("Cache-Miss-Test", 10);
        String key = RedisDropCache.key(drop.getId());

        try {
            assertThat(redisTemplate.hasKey(key)).isFalse();
            double missesBefore = cacheRequests("miss");

            Drop loaded = dropService.getDrop(drop.getId());

            assertThat(loaded.getName()).isEqualTo("Cache-Miss-Test");
            assertThat(cacheRequests("miss")).isEqualTo(missesBefore + 1);
            assertThat(redisTemplate.opsForValue().get(key)).contains(drop.getId());

            Long ttl = redisTemplate.getExpire(key, TimeUnit.SECONDS);
            assertThat(ttl).isNotNull().isPositive().isLessThanOrEqualTo(30);
        } finally {
            redisTemplate.delete(key);
        }
    }

    @Test
    void hitIsServedFromRedisWithoutTouchingDatabase() {
        Drop drop = dropService.createDrop("Cache-Hit-Test", 10);
        String key = RedisDropCache.key(drop.getId());

        try {
            dropService.getDrop(drop.getId());

            // Overwrite the cached entry: if the next read returns it, it did not come from PostgreSQL.
            redisTemplate.opsForValue().set(key, """
                    {"id":"%s","name":"Served-From-Cache","totalInventory":10,"allocatedCount":0,"status":"OPEN"}
                    """.formatted(drop.getId()));
            double hitsBefore = cacheRequests("hit");

            Drop cached = dropService.getDrop(drop.getId());

            assertThat(cached.getName()).isEqualTo("Served-From-Cache");
            assertThat(cacheRequests("hit")).isEqualTo(hitsBefore + 1);
        } finally {
            redisTemplate.delete(key);
        }
    }

    @Test
    void allocationEvictsCachedDropAfterCommit() {
        Drop drop = dropService.createDrop("Cache-Evict-Test", 2);
        String key = RedisDropCache.key(drop.getId());

        try {
            dropService.joinDrop(drop.getId(), "USER-A");
            assertThat(dropService.getDrop(drop.getId()).getAllocatedCount()).isZero();
            assertThat(redisTemplate.hasKey(key)).isTrue();

            assertThat(dropService.allocateNext(drop.getId()).getStatus()).isEqualTo(AllocationStatus.ALLOCATED);

            assertThat(redisTemplate.hasKey(key)).isFalse();
            assertThat(dropService.getDrop(drop.getId()).getAllocatedCount()).isEqualTo(1);
        } finally {
            redisTemplate.delete(key);
        }
    }

    @Test
    void rolledBackAllocationLeavesCacheUntouched() {
        Drop drop = dropService.createDrop("Cache-Rollback-Test", 2);
        String key = RedisDropCache.key(drop.getId());

        try {
            dropService.joinDrop(drop.getId(), "USER-A");
            assertThat(redisTemplate.hasKey(key)).isTrue();

            transactionTemplate.executeWithoutResult(status -> {
                dropService.allocateNext(drop.getId());
                status.setRollbackOnly();
            });

            assertThat(redisTemplate.hasKey(key)).isTrue();
            assertThat(dropService.getDrop(drop.getId()).getAllocatedCount()).isZero();
        } finally {
            redisTemplate.delete(key);
        }
    }

    private double cacheRequests(String result) {
        Counter counter = meterRegistry.find("fairdrop.cache.requests")
                .tag("cache", "drop")
                .tag("result", result)
                .counter();
        return counter == null ? 0 : counter.count();
    }
}
