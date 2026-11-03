package com.fairdrop.messaging;

import com.fairdrop.observability.FairDropMetrics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Topic declarations and the consumer error-handling policy: bounded exponential retries, then the
 * record is parked on {@code <topic><dead-letter-suffix>} so one poison message cannot stall a
 * partition. Topics are only created when {@code spring.kafka.admin.auto-create} is enabled.
 */
@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Bean
    public NewTopic allocationEventsTopic(FairDropKafkaProperties properties) {
        return TopicBuilder.name(properties.allocationEventsTopic())
                .partitions(properties.topicPartitions())
                .replicas(properties.topicReplicas())
                .build();
    }

    @Bean
    public NewTopic allocationEventsDeadLetterTopic(FairDropKafkaProperties properties) {
        return TopicBuilder.name(properties.deadLetterTopic(properties.allocationEventsTopic()))
                .partitions(properties.topicPartitions())
                .replicas(properties.topicReplicas())
                .build();
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, String> kafkaTemplate,
            FairDropKafkaProperties properties,
            FairDropMetrics metrics) {

        // A negative partition lets the producer choose one from the record key.
        DeadLetterPublishingRecoverer deadLetterPublisher = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(properties.deadLetterTopic(record.topic()), -1));

        ConsumerRecordRecoverer recoverer = (record, exception) -> {
            log.error("Dead-lettering record topic={} partition={} offset={} key={}",
                    record.topic(), record.partition(), record.offset(), record.key(), exception);
            deadLetterPublisher.accept(record, exception);
            metrics.eventDeadLettered(record.topic());
        };

        FairDropKafkaProperties.ConsumerRetry retry = properties.consumerRetry();
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(retry.maxRetries());
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(MalformedEventException.class);
        return errorHandler;
    }
}
