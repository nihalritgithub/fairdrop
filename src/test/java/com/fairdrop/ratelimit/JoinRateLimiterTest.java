package com.fairdrop.ratelimit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class JoinRateLimiterTest {

    @Autowired
    private JoinRateLimiter joinRateLimiter;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    void shouldAllowFiveRequestsAndRejectSixth() {

        String dropId = "drop-" + UUID.randomUUID();
        String userId = "user-" + UUID.randomUUID();

        String key = "rate:join:" + dropId + ":" + userId;

        try {
            assertTrue(joinRateLimiter.allow(dropId, userId));
            assertTrue(joinRateLimiter.allow(dropId, userId));
            assertTrue(joinRateLimiter.allow(dropId, userId));
            assertTrue(joinRateLimiter.allow(dropId, userId));
            assertTrue(joinRateLimiter.allow(dropId, userId));

            assertFalse(joinRateLimiter.allow(dropId, userId));

        } finally {
            redisTemplate.delete(key);
        }
    }

    @Test
    void shouldSetExpiryOnRateLimitKey() {

        String dropId = "drop-" + UUID.randomUUID();
        String userId = "user-" + UUID.randomUUID();

        String key = "rate:join:" + dropId + ":" + userId;

        try {
            assertTrue(joinRateLimiter.allow(dropId, userId));

            Long ttl =
                    redisTemplate.getExpire(
                            key,
                            TimeUnit.SECONDS
                    );

            assertNotNull(ttl);
            assertTrue(ttl > 0);
            assertTrue(ttl <= 60);

        } finally {
            redisTemplate.delete(key);
        }
    }

    @Test
    void concurrentRequestsShouldAllowExactlyFive() throws Exception {

        String dropId = "drop-" + UUID.randomUUID();
        String userId = "user-" + UUID.randomUUID();

        String key = "rate:join:" + dropId + ":" + userId;

        int requestCount = 20;

        ExecutorService executor =
                Executors.newFixedThreadPool(requestCount);

        CountDownLatch ready =
                new CountDownLatch(requestCount);

        CountDownLatch start =
                new CountDownLatch(1);

        List<Future<Boolean>> futures =
                new ArrayList<>();

        try {
            for (int i = 0; i < requestCount; i++) {

                futures.add(
                        executor.submit(() -> {

                            ready.countDown();

                            start.await();

                            return joinRateLimiter.allow(
                                    dropId,
                                    userId
                            );
                        })
                );
            }

            ready.await();

            start.countDown();

            int allowedCount = 0;

            for (Future<Boolean> future : futures) {

                if (future.get()) {
                    allowedCount++;
                }
            }

            assertEquals(5, allowedCount);

        } finally {
            executor.shutdownNow();
            redisTemplate.delete(key);
        }
    }
}