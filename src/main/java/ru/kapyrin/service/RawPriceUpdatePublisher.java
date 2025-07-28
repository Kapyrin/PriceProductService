package ru.kapyrin.service;

public interface RawPriceUpdatePublisher {
    void publishRawPriceUpdate(String rawJsonBody);
    void shutdown();
}
