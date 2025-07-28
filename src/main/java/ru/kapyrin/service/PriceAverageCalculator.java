package ru.kapyrin.service;

public interface PriceAverageCalculator {
    Double getAveragePrice(Long productId);
    void updateAveragePriceCaches(Long productId, Double newAveragePrice);
}
