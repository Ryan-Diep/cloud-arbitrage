package com.cloud.arbitrage;

import crypto.TickerOuterClass.Ticker;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
public class ArbitrageService {
    
    private static final Logger log = LoggerFactory.getLogger(ArbitrageService.class);

    private static final double MIN_SPREAD_THRESHOLD = 0.5; 
    private static final int STALE_DATA_MS = 5000; 
    private static final int MAX_PRICE_UPDATES = 100;
    
    // Stablecoins that can be cross-compared
    private static final Set<String> STABLECOINS = Set.of("USD", "USDT", "USDC", "DAI");
    
    private final Map<String, ExchangePrice> coinbasePrices = new ConcurrentHashMap<>();
    private final Map<String, ExchangePrice> binancePrices = new ConcurrentHashMap<>();
    private final Map<String, ArbitrageOpportunity> opportunities = new ConcurrentHashMap<>();
    private final Deque<PriceUpdate> recentUpdates = new ConcurrentLinkedDeque<>();
    private final AtomicLong messageCount = new AtomicLong(0);
    private final AtomicLong arbitrageChecks = new AtomicLong(0);

    @KafkaListener(topics = "crypto-prices", groupId = "arbitrage-group")
    public void consume(List<byte[]> messages) {
        long startTime = System.currentTimeMillis();
        int processedCount = 0;
        
        try {
            for (byte[] message : messages) {
                try {
                    Ticker ticker = Ticker.parseFrom(message);
                    long currentTime = System.currentTimeMillis();
                    
                    ExchangePrice exchangePrice = new ExchangePrice(
                        ticker.getPrice(), 
                        ticker.getTimestamp(), 
                        currentTime
                    );
                    
                    // Store price update
                    if ("COINBASE".equals(ticker.getSource())) {
                        coinbasePrices.put(ticker.getSymbol(), exchangePrice);
                    } else if ("BINANCE".equals(ticker.getSource())) {
                        binancePrices.put(ticker.getSymbol(), exchangePrice);
                    }
                    
                    // Add to recent updates feed (throttle this - don't add every message)
                    if (processedCount % 10 == 0) {
                        PriceUpdate update = new PriceUpdate(
                            ticker.getSymbol(),
                            ticker.getSource(),
                            ticker.getPrice(),
                            ticker.getTimestamp()
                        );
                        recentUpdates.addFirst(update);
                        
                        // Keep only last MAX_PRICE_UPDATES
                        while (recentUpdates.size() > MAX_PRICE_UPDATES) {
                            recentUpdates.removeLast();
                        }
                    }
                    
                    // Only check arbitrage every 10th message to reduce CPU
                    if (processedCount % 10 == 0) {
                        detectArbitrage(ticker.getSymbol(), currentTime);
                        detectCrossQuoteArbitrage(ticker.getSymbol(), currentTime);
                    }
                    
                    processedCount++;
                } catch (Exception e) {
                    log.error("Error processing individual message: {}", e.getMessage());
                }
            }
            
            long totalCount = messageCount.addAndGet(processedCount);
            long duration = System.currentTimeMillis() - startTime;
            
            // Log batch stats every 1000 messages
            if (totalCount % 1000 < processedCount) {
                log.info("📦 Batch: {} messages in {}ms ({} msg/sec) | Total: {} | Tracking {} CB, {} BN, {} opps",
                    processedCount, duration, 
                    processedCount * 1000 / Math.max(1, duration),
                    totalCount,
                    coinbasePrices.size(), binancePrices.size(), opportunities.size());
            }
            
        } catch (Exception e) {
            log.error("Error processing batch: {}", e.getMessage(), e);
        }
    }
    
    private void detectArbitrage(String symbol, long currentTime) {
        arbitrageChecks.incrementAndGet();
        
        ExchangePrice coinbasePrice = coinbasePrices.get(symbol);
        ExchangePrice binancePrice = binancePrices.get(symbol);
        
        if (coinbasePrice == null || binancePrice == null) {
            // Log missing data occasionally for debugging
            if (arbitrageChecks.get() % 1000 == 0) {
                log.debug("Symbol {} missing on one exchange. CB: {}, BN: {}", 
                    symbol, (coinbasePrice != null), (binancePrice != null));
            }
            opportunities.remove(symbol);
            return;
        }
        
        // Check data freshness - BOTH must be recent
        long cbAge = currentTime - coinbasePrice.receivedAt;
        long bnAge = currentTime - binancePrice.receivedAt;
        
        if (cbAge > STALE_DATA_MS || bnAge > STALE_DATA_MS) {
            // Log stale data for debugging
            if (cbAge > STALE_DATA_MS || bnAge > STALE_DATA_MS) {
                log.debug("Stale data for {}: CB age={}ms, BN age={}ms", symbol, cbAge, bnAge);
            }
            opportunities.remove(symbol);
            return;
        }
        
        double cbPrice = coinbasePrice.price;
        double bnPrice = binancePrice.price;
        
        double priceDiff = Math.abs(cbPrice - bnPrice);
        
        // Skip if price difference is negligible
        if (priceDiff < 0.01) {
            opportunities.remove(symbol);
            return;
        }
        
        double spreadPercent;
        String buyExchange;
        String sellExchange;
        double buyPrice;
        double sellPrice;
        
        // Calculate spread percentage correctly
        if (cbPrice < bnPrice) {
            // Buy on Coinbase, sell on Binance
            spreadPercent = ((bnPrice - cbPrice) / cbPrice) * 100;
            buyExchange = "COINBASE";
            sellExchange = "BINANCE";
            buyPrice = cbPrice;
            sellPrice = bnPrice;
        } else {
            // Buy on Binance, sell on Coinbase
            spreadPercent = ((cbPrice - bnPrice) / bnPrice) * 100;
            buyExchange = "BINANCE";
            sellExchange = "COINBASE";
            buyPrice = bnPrice;
            sellPrice = cbPrice;
        }
        
        // Log all spreads above 0.5% for debugging
        if (spreadPercent >= 0.5) {
            log.info("SPREAD DETECTED: {} - {}% | CB: ${} (age: {}ms) | BN: ${} (age: {}ms) | Buy: {} | Sell: {} | PASSED_THRESHOLD: {}", 
                symbol, String.format("%.2f", spreadPercent), cbPrice, cbAge, bnPrice, bnAge, buyExchange, sellExchange, 
                spreadPercent >= MIN_SPREAD_THRESHOLD);
        }
        
        if (spreadPercent >= MIN_SPREAD_THRESHOLD) {
            ArbitrageOpportunity opp = new ArbitrageOpportunity(
                symbol,
                buyExchange,
                sellExchange,
                buyPrice,
                sellPrice,
                spreadPercent,
                priceDiff,
                currentTime
            );
            opportunities.put(symbol, opp);
            
            // Always log opportunities above threshold
            log.warn("⚡ ARBITRAGE OPPORTUNITY STORED: {} - {}% spread | Buy @ {} ${} | Sell @ {} ${} | Profit: ${} | OppsMap size: {}", 
                symbol, String.format("%.2f", spreadPercent), buyExchange, buyPrice, sellExchange, sellPrice, 
                String.format("%.2f", priceDiff), opportunities.size());
        } else {
            opportunities.remove(symbol);
        }
    }
    
    private void detectCrossQuoteArbitrage(String symbol, long currentTime) {
        // Parse the symbol to get base and quote
        String[] parts = symbol.split("-");
        if (parts.length != 2) return;
        
        String baseAsset = parts[0];
        String quoteAsset = parts[1];
        
        // Only check if this is a stablecoin pair
        if (!STABLECOINS.contains(quoteAsset)) return;
        
        // Find all other stablecoin pairs for this base asset
        for (String otherQuote : STABLECOINS) {
            if (otherQuote.equals(quoteAsset)) continue;
            
            String otherSymbol = baseAsset + "-" + otherQuote;
            
            // Try all combinations: CB symbol1 vs BN symbol2, BN symbol1 vs CB symbol2
            checkCrossQuotePair(symbol, otherSymbol, currentTime, true);  // CB1 vs BN2
            checkCrossQuotePair(symbol, otherSymbol, currentTime, false); // BN1 vs CB2
        }
    }
    
    private void checkCrossQuotePair(String symbol1, String symbol2, long currentTime, boolean cb1_bn2) {
        ExchangePrice price1, price2;
        String exchange1, exchange2;
        
        if (cb1_bn2) {
            price1 = coinbasePrices.get(symbol1);
            price2 = binancePrices.get(symbol2);
            exchange1 = "COINBASE";
            exchange2 = "BINANCE";
        } else {
            price1 = binancePrices.get(symbol1);
            price2 = coinbasePrices.get(symbol2);
            exchange1 = "BINANCE";
            exchange2 = "COINBASE";
        }
        
        if (price1 == null || price2 == null) return;
        
        // Check freshness
        long age1 = currentTime - price1.receivedAt;
        long age2 = currentTime - price2.receivedAt;
        if (age1 > STALE_DATA_MS || age2 > STALE_DATA_MS) return;
        
        double p1 = price1.price;
        double p2 = price2.price;
        
        // Calculate spread - accounting for different stablecoins
        // Assumption: USDT ≈ USDC ≈ USD ≈ DAI ≈ $1
        double priceDiff = Math.abs(p1 - p2);
        if (priceDiff < 0.01) return;
        
        double spreadPercent = (priceDiff / Math.min(p1, p2)) * 100;
        
        // Create unique key for this cross-quote opportunity
        String oppKey = symbol1 + "_vs_" + symbol2 + "_" + exchange1 + "_" + exchange2;
        
        if (spreadPercent >= MIN_SPREAD_THRESHOLD) {
            String buyExchange, sellExchange, buySymbol, sellSymbol;
            double buyPrice, sellPrice;
            
            if (p1 < p2) {
                buyExchange = exchange1;
                sellExchange = exchange2;
                buySymbol = symbol1;
                sellSymbol = symbol2;
                buyPrice = p1;
                sellPrice = p2;
            } else {
                buyExchange = exchange2;
                sellExchange = exchange1;
                buySymbol = symbol2;
                sellSymbol = symbol1;
                buyPrice = p2;
                sellPrice = p1;
            }
            
            // Format display: "BTC-USDT/BTC-USDC" to show cross-quote
            String displaySymbol = buySymbol + "/" + sellSymbol;
            
            ArbitrageOpportunity opp = new ArbitrageOpportunity(
                displaySymbol,
                buyExchange,
                sellExchange,
                buyPrice,
                sellPrice,
                spreadPercent,
                priceDiff,
                currentTime
            );
            opportunities.put(oppKey, opp);
            
            log.info("⚡ CROSS-QUOTE ARBITRAGE: {} - {}% | Buy {} @ {} ${} | Sell {} @ {} ${}", 
                displaySymbol, String.format("%.2f", spreadPercent), 
                buySymbol, buyExchange, buyPrice, 
                sellSymbol, sellExchange, sellPrice);
        }
    }
    
    public Map<String, Object> getOpportunitiesData() {
        long currentTime = System.currentTimeMillis();
        
        log.debug("getOpportunitiesData() called. Current opportunities map size: {}", opportunities.size());
        
        // Remove stale opportunities (use same threshold as detection)
        int removedCount = 0;
        Iterator<Map.Entry<String, ArbitrageOpportunity>> iterator = opportunities.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ArbitrageOpportunity> entry = iterator.next();
            if (currentTime - entry.getValue().timestamp > STALE_DATA_MS) {
                log.debug("Removing stale opportunity: {} (age: {}ms)", entry.getKey(), currentTime - entry.getValue().timestamp);
                iterator.remove();
                removedCount++;
            }
        }
        
        if (removedCount > 0) {
            log.info("Removed {} stale opportunities. Remaining: {}", removedCount, opportunities.size());
        }
        
        List<Map<String, Object>> oppList = opportunities.values().stream()
            .sorted(Comparator.comparingDouble(ArbitrageOpportunity::getSpreadPercent).reversed())
            .limit(10)
            .map(opp -> {
                Map<String, Object> map = new HashMap<>();
                map.put("symbol", opp.symbol);
                map.put("buyExchange", opp.buyExchange);
                map.put("sellExchange", opp.sellExchange);
                map.put("buyPrice", opp.buyPrice);
                map.put("sellPrice", opp.sellPrice);
                map.put("spread", opp.spreadPercent);
                map.put("priceDiff", opp.priceDiff);
                map.put("timestamp", opp.timestamp);
                return map;
            })
            .collect(Collectors.toList());
        
        log.info("Returning {} opportunities to API (top 10 of {})", oppList.size(), opportunities.size());
        
        double avgSpread = opportunities.values().stream()
            .mapToDouble(ArbitrageOpportunity::getSpreadPercent)
            .average()
            .orElse(0.0);
        
        double maxSpread = opportunities.values().stream()
            .mapToDouble(ArbitrageOpportunity::getSpreadPercent)
            .max()
            .orElse(0.0);
        
        Map<String, Object> stats = new HashMap<>();
        stats.put("avgSpread", avgSpread);
        stats.put("maxSpread", maxSpread);
        stats.put("trackedSymbols", Math.max(coinbasePrices.size(), binancePrices.size()));
        stats.put("activeOpportunities", opportunities.size());
        
        Map<String, Object> result = new HashMap<>();
        result.put("opportunities", oppList);
        result.put("stats", stats);
        result.put("lastUpdate", currentTime);
        
        return result;
    }
    
    public Map<String, Object> getAllPricesData() {
        long currentTime = System.currentTimeMillis();
        
        // Build a map of all symbols with prices from both exchanges
        Map<String, Map<String, Object>> allPrices = new HashMap<>();
        
        // Add Coinbase prices - show all, mark age
        coinbasePrices.forEach((symbol, price) -> {
            allPrices.computeIfAbsent(symbol, k -> new HashMap<>());
            allPrices.get(symbol).put("coinbasePrice", price.price);
            allPrices.get(symbol).put("coinbaseTimestamp", price.timestamp);
            allPrices.get(symbol).put("coinbaseAge", currentTime - price.receivedAt);
        });
        
        // Add Binance prices - show all, mark age
        binancePrices.forEach((symbol, price) -> {
            allPrices.computeIfAbsent(symbol, k -> new HashMap<>());
            allPrices.get(symbol).put("binancePrice", price.price);
            allPrices.get(symbol).put("binanceTimestamp", price.timestamp);
            allPrices.get(symbol).put("binanceAge", currentTime - price.receivedAt);
        });
        
        // Convert to list format
        List<Map<String, Object>> pricesList = allPrices.entrySet().stream()
            .map(entry -> {
                Map<String, Object> priceData = new HashMap<>(entry.getValue());
                priceData.put("symbol", entry.getKey());
                
                // Calculate spread if both prices exist
                if (priceData.containsKey("coinbasePrice") && priceData.containsKey("binancePrice")) {
                    double cbPrice = (double) priceData.get("coinbasePrice");
                    double bnPrice = (double) priceData.get("binancePrice");
                    double diff = Math.abs(cbPrice - bnPrice);
                    // Use the buy price as denominator for correct spread calculation
                    double minPrice = Math.min(cbPrice, bnPrice);
                    double spreadPercent = (diff / minPrice) * 100;
                    priceData.put("spreadPercent", spreadPercent);
                    
                    // Mark if both prices are fresh
                    long cbAge = (long) priceData.get("coinbaseAge");
                    long bnAge = (long) priceData.get("binanceAge");
                    priceData.put("bothFresh", cbAge <= STALE_DATA_MS && bnAge <= STALE_DATA_MS);
                }
                
                return priceData;
            })
            .sorted(Comparator.comparing(m -> (String) m.get("symbol")))
            .collect(Collectors.toList());
        
        // Get recent price updates for live feed
        List<Map<String, Object>> updates = recentUpdates.stream()
            .limit(100)
            .map(update -> {
                Map<String, Object> map = new HashMap<>();
                map.put("symbol", update.symbol);
                map.put("exchange", update.exchange);
                map.put("price", update.price);
                map.put("timestamp", update.timestamp);
                return map;
            })
            .collect(Collectors.toList());
        
        Map<String, Object> result = new HashMap<>();
        result.put("prices", pricesList);
        result.put("updates", updates);
        result.put("totalSymbols", allPrices.size());
        result.put("lastUpdate", currentTime);
        
        return result;
    }
    
    private static class ExchangePrice {
        final double price;
        final long timestamp;
        final long receivedAt;
        
        ExchangePrice(double price, long timestamp, long receivedAt) {
            this.price = price;
            this.timestamp = timestamp;
            this.receivedAt = receivedAt;
        }
    }
    
    private static class PriceUpdate {
        final String symbol;
        final String exchange;
        final double price;
        final long timestamp;
        
        PriceUpdate(String symbol, String exchange, double price, long timestamp) {
            this.symbol = symbol;
            this.exchange = exchange;
            this.price = price;
            this.timestamp = timestamp;
        }
    }
    
    private static class ArbitrageOpportunity {
        final String symbol;
        final String buyExchange;
        final String sellExchange;
        final double buyPrice;
        final double sellPrice;
        final double spreadPercent;
        final double priceDiff;
        final long timestamp;
        
        ArbitrageOpportunity(String symbol, String buyExchange, String sellExchange,
                           double buyPrice, double sellPrice, double spreadPercent, 
                           double priceDiff, long timestamp) {
            this.symbol = symbol;
            this.buyExchange = buyExchange;
            this.sellExchange = sellExchange;
            this.buyPrice = buyPrice;
            this.sellPrice = sellPrice;
            this.spreadPercent = spreadPercent;
            this.priceDiff = priceDiff;
            this.timestamp = timestamp;
        }
        
        double getSpreadPercent() {
            return spreadPercent;
        }
        
        double getPriceDiff() {
            return priceDiff;
        }
    }
}