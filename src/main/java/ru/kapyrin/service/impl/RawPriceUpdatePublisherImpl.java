package ru.kapyrin.service.impl;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.RabbitMQConfig;
import ru.kapyrin.service.RawPriceUpdatePublisher;

import java.io.IOException;

@Slf4j
public class RawPriceUpdatePublisherImpl implements RawPriceUpdatePublisher {

    private final Channel rabbitMQChannel;
    private final String exchangeName;
    private final String rawRoutingKey;

    public RawPriceUpdatePublisherImpl(RabbitMQConfig rabbitMQConfig) {
        this.rabbitMQChannel = rabbitMQConfig.getChannel();
        this.exchangeName = rabbitMQConfig.getExchangeName();
        this.rawRoutingKey = rabbitMQConfig.getRawRoutingKey();
        log.info("RawPriceUpdatePublisherImpl initialized for publishing raw messages to RabbitMQ.");
    }

    @Override
    public void publishRawPriceUpdate(String rawJsonBody) {
        log.debug("Received raw JSON body for publishing to RabbitMQ.");
        try {
            rabbitMQChannel.basicPublish(exchangeName, rawRoutingKey, new AMQP.BasicProperties.Builder().deliveryMode(2).build(), rawJsonBody.getBytes("UTF-8"));
            log.debug("Published raw JSON body to RabbitMQ.");
        } catch (IOException e) {
            log.error("Failed to publish raw JSON body to RabbitMQ: {}", e.getMessage(), e);
        } catch (Exception e) {
            log.error("An unexpected error occurred during publishing raw JSON body: {}", e.getMessage(), e);
        }
    }

    @Override
    public void shutdown() {
        log.info("RawPriceUpdatePublisher shutdown complete.");
    }
}