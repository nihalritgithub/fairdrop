package com.fairdrop.controller;

import com.fairdrop.exception.RateLimiterUnavailableException;
import com.fairdrop.model.QueueEntry;
import com.fairdrop.observability.CorrelationId;
import com.fairdrop.ratelimit.JoinRateLimiter;
import com.fairdrop.service.DropService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.matchesPattern;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP contract of the traffic-protection paths: status codes, error codes and the fact that no
 * infrastructure exception reaches the client.
 */
@WebMvcTest(DropController.class)
class DropControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private DropService dropService;

    @MockitoBean
    private JoinRateLimiter joinRateLimiter;

    @Test
    void joinWithinLimitReturnsQueuePosition() throws Exception {
        when(joinRateLimiter.allow("drop-1", "user-1")).thenReturn(true);
        when(dropService.joinDrop("drop-1", "user-1")).thenReturn(new QueueEntry("user-1", 7));

        mockMvc.perform(post("/drops/drop-1/join").param("userId", "user-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sequenceNumber").value(7));
    }

    @Test
    void joinOverLimitReturns429WithoutTouchingTheQueue() throws Exception {
        when(joinRateLimiter.allow("drop-1", "user-1")).thenReturn(false);

        mockMvc.perform(post("/drops/drop-1/join").param("userId", "user-1"))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.code").value("RATE_LIMIT_EXCEEDED"));

        verifyNoInteractions(dropService);
    }

    @Test
    void rateLimiterOutageReturns503() throws Exception {
        when(joinRateLimiter.allow(anyString(), anyString()))
                .thenThrow(new RateLimiterUnavailableException(new RedisConnectionFailureException("down")));

        mockMvc.perform(post("/drops/drop-1/join").param("userId", "user-1"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.code").value("RATE_LIMITER_UNAVAILABLE"))
                .andExpect(jsonPath("$.message").value(
                        "Join service is temporarily unavailable. Please try again later."));

        verifyNoInteractions(dropService);
    }

    @Test
    void oversizedUserIdIsRejectedBeforeRateLimiting() throws Exception {
        mockMvc.perform(post("/drops/drop-1/join").param("userId", "u".repeat(256)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.errors.userId").exists());

        verifyNoInteractions(joinRateLimiter, dropService);
    }

    @Test
    void transientDatabaseFailureReturns503() throws Exception {
        when(dropService.allocateNext("drop-1")).thenThrow(new PessimisticLockingFailureException("lock timeout"));

        mockMvc.perform(post("/drops/drop-1/allocate-next"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.code").value("TEMPORARILY_UNAVAILABLE"));
    }

    @Test
    void correlationIdIsEchoedOrGenerated() throws Exception {
        when(dropService.getQueueEntry("drop-1", "user-1")).thenReturn(new QueueEntry("user-1", 1));

        mockMvc.perform(get("/drops/drop-1/queue/user-1").header(CorrelationId.HTTP_HEADER, "client-abc-123"))
                .andExpect(header().string(CorrelationId.HTTP_HEADER, "client-abc-123"));

        mockMvc.perform(get("/drops/drop-1/queue/user-1").header(CorrelationId.HTTP_HEADER, "bad value\r\n"))
                .andExpect(header().string(CorrelationId.HTTP_HEADER, matchesPattern("[0-9a-f-]{36}")));
    }
}
