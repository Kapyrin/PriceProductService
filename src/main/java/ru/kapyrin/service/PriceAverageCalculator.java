package ru.kapyrin.service;

import java.util.concurrent.CompletableFuture;

public interface PriceAverageCalculator {
    CompletableFuture<Double> getAveragePriceAsync(Long productId);
    void updateAveragePriceCaches(Long productId, Double newAveragePrice);
}