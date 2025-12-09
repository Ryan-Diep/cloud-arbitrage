package com.cloud.arbitrage;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@CrossOrigin(origins = "*")
public class ArbitrageController {
    
    private final ArbitrageService arbitrageService;
    
    public ArbitrageController(ArbitrageService arbitrageService) {
        this.arbitrageService = arbitrageService;
    }
    
    @GetMapping("/api/opportunities")
    public Map<String, Object> getOpportunities() {
        return arbitrageService.getOpportunitiesData();
    }
    
    @GetMapping("/api/prices")
    public Map<String, Object> getAllPrices() {
        return arbitrageService.getAllPricesData();
    }
}