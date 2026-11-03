package com.fairdrop;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Spring Boot entry point for the FairDrop service, which lets users join a limited-inventory
 * drop and receive a fair, first-come-first-served position in its queue.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class FairdropApplication {

    /**
     * Boots the Spring application context and starts the embedded web server.
     *
     * @param args command-line arguments passed through to Spring
     */
    public static void main(String[] args) {
        SpringApplication.run(FairdropApplication.class, args);
    }
}
