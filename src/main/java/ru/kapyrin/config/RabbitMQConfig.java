package ru.kapyrin.config;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

@Slf4j
@Getter
public class RabbitMQConfig {
    private final Connection connection;
    private final Channel channel;
    private final String rawQueueName;
    private final String validatedQueueName;
    private final String dlqName;
    private final String exchangeName;
    private final String rawRoutingKey;
    private final String validatedRoutingKey;

    public RabbitMQConfig(PropertiesLoader propertiesLoader) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(propertiesLoader.getProperty("rabbitmq.host", "localhost"));
        factory.setPort(propertiesLoader.getIntProperty("rabbitmq.port", 5672));
        factory.setUsername(propertiesLoader.getProperty("rabbitmq.username", "guest"));
        factory.setPassword(propertiesLoader.getProperty("rabbitmq.password", "guest"));

        this.rawQueueName = propertiesLoader.getProperty("rabbitmq.raw.queue.name", "raw_price_updates_queue");
        this.validatedQueueName = propertiesLoader.getProperty("rabbitmq.validated.queue.name", "validated_price_updates_queue");
        this.dlqName = propertiesLoader.getProperty("rabbitmq.dlq.name", "price_updates_dlq");
        this.exchangeName = propertiesLoader.getProperty("rabbitmq.exchange.name", "price_updates_exchange");
        this.rawRoutingKey = propertiesLoader.getProperty("rabbitmq.raw.routing.key", "price.update.raw");
        this.validatedRoutingKey = propertiesLoader.getProperty("rabbitmq.validated.routing.key", "price.update.validated");

        try {
            this.connection = factory.newConnection();
            this.channel = connection.createChannel();

            channel.exchangeDeclare(exchangeName, "topic", true);

            Map<String, Object> args = new HashMap<>();
            args.put("x-message-ttl", propertiesLoader.getIntProperty("rabbitmq.dlq.ttl.hours", 12) * 3600 * 1000);

            channel.queueDeclare(rawQueueName, true, false, false, null);
            channel.queueBind(rawQueueName, exchangeName, rawRoutingKey);
            log.info("RabbitMQ raw queue '{}' bound to exchange '{}' with routing key '{}'", rawQueueName, exchangeName, rawRoutingKey);

            channel.queueDeclare(validatedQueueName, true, false, false, null);
            channel.queueBind(validatedQueueName, exchangeName, validatedRoutingKey);
            log.info("RabbitMQ validated queue '{}' bound to exchange '{}' with routing key '{}'", validatedQueueName, exchangeName, validatedRoutingKey);

            channel.queueDeclare(dlqName, true, false, false, args);
            log.info("RabbitMQ DLQ '{}' declared with TTL {} hours", dlqName, propertiesLoader.getIntProperty("rabbitmq.dlq.ttl.hours", 12));
        } catch (IOException | TimeoutException e) {
            log.error("Failed to connect to RabbitMQ or declare queues/exchange", e);
            throw new RuntimeException("Failed to initialize RabbitMQ", e);
        }
    }

    public void close() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
                log.info("RabbitMQ channel closed");
            }
            if (connection != null && connection.isOpen()) {
                connection.close();
                log.info("RabbitMQ connection closed");
            }
        } catch (IOException | TimeoutException e) {
            log.error("Error closing RabbitMQ resources", e);
        }
    }
}