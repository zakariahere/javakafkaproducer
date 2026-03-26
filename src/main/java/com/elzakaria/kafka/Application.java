package com.elzakaria.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {
    "com.elzakaria.kafka",
    "com.elzakaria.kafkaproducer",
    "com.elzakaria.kafkaconsumer"
})
public class Application {
    static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}

