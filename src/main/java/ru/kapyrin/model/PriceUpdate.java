package ru.kapyrin.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PriceUpdate(
        @JsonProperty("product_id") long productId,
        @JsonProperty("manufacturer_name") String manufacturerName,
        double price) {
}