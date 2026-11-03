package com.fairdrop.config;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.lettuce.core.ClientOptions;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.data.redis.autoconfigure.LettuceClientConfigurationBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;

@Configuration
@EnableScheduling
public class InfrastructureConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry(MeterRegistry meterRegistry) {
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.ofDefaults();
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry).bindTo(meterRegistry);
        return registry;
    }

    /**
     * By default Lettuce queues commands while reconnecting, so every caller waits for the full
     * command timeout during a Redis outage. Rejecting them immediately lets the rate limiter fail
     * closed and the cache fail open without stalling request threads.
     */
    @Bean
    public LettuceClientConfigurationBuilderCustomizer rejectCommandsWhileDisconnected() {
        return builder -> {
            ClientOptions current = builder.build().getClientOptions().orElseGet(ClientOptions::create);
            builder.clientOptions(current.mutate()
                    .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                    .build());
        };
    }
}
