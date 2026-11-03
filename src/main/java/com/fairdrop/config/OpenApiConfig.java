package com.fairdrop.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI fairDropOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("FairDrop API")
                        .version("1.0.0")
                        .description(
                                "High-concurrency fair allocation service with PostgreSQL locking, "
                                        + "Redis traffic protection/cache, Kafka, and a transactional outbox.")
                        .contact(new Contact().name("Nihal Kumar Sharma")));
    }
}
