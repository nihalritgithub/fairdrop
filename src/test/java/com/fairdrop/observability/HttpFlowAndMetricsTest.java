package com.fairdrop.observability;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.micrometer.metrics.test.autoconfigure.AutoConfigureMetrics;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Drives the public API end to end over HTTP and checks that the resulting metrics are exported in
 * Prometheus format.
 */
@SpringBootTest
@AutoConfigureMockMvc
@AutoConfigureMetrics
class HttpFlowAndMetricsTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JsonMapper jsonMapper;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    void joinRateLimitAndAllocationOverHttp() throws Exception {
        String response = mockMvc.perform(post("/drops")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"Http-Flow-Test\",\"inventory\":1}"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();
        String dropId = jsonMapper.readTree(response).get("id").asString();
        String userId = "user-" + UUID.randomUUID();

        try {
            for (int i = 0; i < 5; i++) {
                mockMvc.perform(post("/drops/{dropId}/join", dropId).param("userId", userId))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.sequenceNumber").value(1));
            }
            mockMvc.perform(post("/drops/{dropId}/join", dropId).param("userId", userId))
                    .andExpect(status().isTooManyRequests())
                    .andExpect(jsonPath("$.code").value("RATE_LIMIT_EXCEEDED"));

            mockMvc.perform(post("/drops/{dropId}/allocate-next", dropId))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.status").value("ALLOCATED"))
                    .andExpect(jsonPath("$.userId").value(userId));
            mockMvc.perform(post("/drops/{dropId}/allocate-next", dropId))
                    .andExpect(jsonPath("$.status").value("SOLD_OUT"));

            mockMvc.perform(get("/drops/{dropId}", dropId))
                    .andExpect(status().isOk())
                    .andExpect(header().exists(CorrelationId.HTTP_HEADER))
                    .andExpect(jsonPath("$.allocatedCount").value(1));
        } finally {
            redisTemplate.delete("rate:join:" + dropId + ":" + userId);
            redisTemplate.delete("fairdrop:cache:drop:" + dropId);
        }
    }

    @Test
    void prometheusEndpointExposesBusinessAndBottleneckMetrics() throws Exception {
        String dropId = UUID.randomUUID().toString();
        mockMvc.perform(get("/drops/{dropId}", dropId)).andExpect(status().isNotFound());

        mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("fairdrop_join_attempts_total")))
                .andExpect(content().string(containsString("fairdrop_join_rate_limited_total")))
                .andExpect(content().string(containsString("fairdrop_ratelimit_unavailable_total")))
                .andExpect(content().string(containsString("fairdrop_allocation_outcomes_total")))
                .andExpect(content().string(containsString("fairdrop_allocation_lock_wait_seconds")))
                .andExpect(content().string(containsString("fairdrop_cache_requests_total")))
                .andExpect(content().string(containsString("fairdrop_outbox_pending")))
                .andExpect(content().string(containsString("hikaricp_connections_pending")))
                .andExpect(content().string(containsString("http_server_requests_seconds_bucket")));
    }

    @Test
    void healthIsUpWithoutLeakingDetails() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"))
                .andExpect(jsonPath("$.components").doesNotExist());
    }
}
