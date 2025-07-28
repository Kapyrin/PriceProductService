package ru.kapyrin.model;

public record ProductAggregates(Double totalSumPrices, Long offerCount) {
    public static ProductAggregates empty() {
        return new ProductAggregates(0.0, 0L);
    }
}
