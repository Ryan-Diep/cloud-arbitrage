package com.cloud.arbitrage;

// The generated code is usually in crypto.TickerOuterClass
import crypto.TickerOuterClass.Ticker;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ArbitrageService {

    private final Map<String, Double> priceCache = new ConcurrentHashMap<>();

    @KafkaListener(topics = "crypto-prices", groupId = "arbitrage-group")
    public void consume(byte[] message) {
        try {
            // 1. Parse the Binary Data
            Ticker ticker = Ticker.parseFrom(message);

            // 2. Calculate Lag
            long latency = System.currentTimeMillis() - ticker.getTimestamp();

            // 3. Print it out
            System.out.printf("✅ [%s] %s Price: $%.2f (Lag: %dms)%n", 
                ticker.getSource(), 
                ticker.getSymbol(), 
                ticker.getPrice(), 
                latency
            );

        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
        }
    }
}