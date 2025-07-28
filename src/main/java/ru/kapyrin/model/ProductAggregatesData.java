package ru.kapyrin.model;

public record ProductAggregatesData(
        Long productId,
        Double averagePrice,
        Double totalSumPrices,
        Long offerCount
) {
    public static ProductAggregatesData empty(Long productId) {
        return new ProductAggregatesData(productId, null, 0.0, 0L);
    }
}