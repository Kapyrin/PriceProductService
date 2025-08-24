package ru.kapyrin.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.PropertiesLoader;
import ru.kapyrin.config.RabbitMQConfig;
import ru.kapyrin.service.MetricsService;
import ru.kapyrin.service.RawPriceUpdatePublisher;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Slf4j
@RequiredArgsConstructor
public class RawPriceUpdatePublisherImpl implements RawPriceUpdatePublisher {
    private final MetricsService metricsService;
    private final Channel channel;
    private final String exchangeName;
    private final String rawRoutingKey;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PropertiesLoader propertiesLoader;

    public RawPriceUpdatePublisherImpl(RabbitMQConfig rabbitMQConfig, MetricsService metricsService, PropertiesLoader propertiesLoader) {
        this.metricsService = metricsService;
        this.exchangeName = rabbitMQConfig.getExchangeName();
        this.rawRoutingKey = rabbitMQConfig.getRawRoutingKey();
        this.propertiesLoader = propertiesLoader;
        try {
            this.channel = rabbitMQConfig.getConnection().createChannel();
            channel.confirmSelect();
            log.info("RawPriceUpdatePublisherImpl initialized with Publisher Confirms");
        } catch (IOException e) {
            log.error("Failed to initialize RabbitMQ channel: {}", e.getMessage());
            throw new RuntimeException("Failed to initialize RawPriceUpdatePublisher", e);
        }
    }

    @Override
    public void publishRawPriceUpdate(String rawJsonBody) {
        int confirmTimeoutMs = propertiesLoader.getIntProperty("rabbitmq.confirm.timeout.ms", 5000); // Значение по умолчанию 5000 мс
        try {
            metricsService.recordPostRequest();
            validateRawPriceUpdate(rawJsonBody);
            channel.basicPublish(exchangeName, rawRoutingKey, new AMQP.BasicProperties.Builder().deliveryMode(2).build(), rawJsonBody.getBytes("UTF-8"));
            if (channel.waitForConfirms(confirmTimeoutMs)) {
                log.debug("Published message to RabbitMQ, size={} bytes", rawJsonBody.length());
            } else {
                log.warn("Message not confirmed by RabbitMQ within {}ms, size={} bytes", confirmTimeoutMs, rawJsonBody.length());
                metricsService.recordPostError();
            }
        } catch (IOException | InterruptedException | TimeoutException e) {
            log.error("Failed to publish message to RabbitMQ: {}", e.getMessage());
            metricsService.recordPostError();
            throw new RuntimeException("Failed to publish raw price update", e);
        }
    }

    @Override
    public void validateRawPriceUpdate(String rawJsonBody) {
        if (rawJsonBody == null || rawJsonBody.isEmpty()) {
            log.warn("Received empty price update body");
            metricsService.recordPostError();
            throw new IllegalArgumentException("Request body cannot be empty");
        }
        try {
            Json.decodeValue(rawJsonBody);
            if (rawJsonBody.length() > 1000 * 1024) {
                log.warn("Batch size exceeds limit of 1MB, size={} bytes", rawJsonBody.length());
                metricsService.recordPostError();
                throw new IllegalArgumentException("Batch size exceeds limit of 1MB");
            }
            metricsService.recordBatchSize((int) Math.ceil(rawJsonBody.length() / 100.0)); // Оценка размера
        } catch (DecodeException e) {
            log.warn("Invalid JSON format, size={} bytes: {}", rawJsonBody.length(), e.getMessage());
            metricsService.recordPostError();
            throw new IllegalArgumentException("Invalid JSON format: " + e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
                log.info("RawPriceUpdatePublisher channel closed");
            }
        } catch (IOException | TimeoutException e) {
            log.error("Failed to close RabbitMQ channel: {}", e.getMessage());
        }
        log.info("RawPriceUpdatePublisher shutdown complete");
    }
}