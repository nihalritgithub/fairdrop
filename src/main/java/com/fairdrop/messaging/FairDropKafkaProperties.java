package com.fairdrop.messaging;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * FairDrop-specific Kafka settings. Connection, serializer and client tuning live under the
 * standard {@code spring.kafka.*} namespace.
 *
 * @param allocationEventsTopic topic carrying {@code AllocationCreated} events, keyed by drop id
 * @param deadLetterSuffix      appended to a topic name to form its dead-letter topic
 * @param topicPartitions       partitions used when the application declares its topics
 * @param topicReplicas         replication factor used when the application declares its topics
 * @param consumerRetry         in-place retries before a record is sent to its dead-letter topic
 */
@Validated
@ConfigurationProperties("fairdrop.kafka")
public record FairDropKafkaProperties(
        @NotBlank @DefaultValue("fairdrop.allocation-events") String allocationEventsTopic,
        @NotBlank @DefaultValue(".dlt") String deadLetterSuffix,
        @Positive @DefaultValue("3") int topicPartitions,
        @Min(1) @DefaultValue("1") short topicReplicas,
        @Valid @NotNull @DefaultValue ConsumerRetry consumerRetry) {

    public String deadLetterTopic(String topic) {
        return topic + deadLetterSuffix;
    }

    public record ConsumerRetry(
            @PositiveOrZero @DefaultValue("4") int maxRetries,
            @NotNull @DurationMin(millis = 10) @DefaultValue("1s") Duration initialInterval,
            @DecimalMin("1.0") @DefaultValue("2.0") double multiplier,
            @NotNull @DurationMin(millis = 10) @DefaultValue("10s") Duration maxInterval) {
    }
}
