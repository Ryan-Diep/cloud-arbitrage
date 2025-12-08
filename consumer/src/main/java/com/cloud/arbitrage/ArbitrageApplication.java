package com.cloud.arbitrage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class ArbitrageApplication {

    public static void main(String[] args) {
        SpringApplication.run(ArbitrageApplication.class, args);
    }
}