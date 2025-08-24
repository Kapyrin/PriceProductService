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
    private final String rawQueueName;
    private final String dlqName;
    private final String exchangeName;
    private final String rawRoutingKey;

    public RabbitMQConfig(PropertiesLoader propertiesLoader) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(propertiesLoader.getProperty("rabbitmq.host", "localhost"));
        factory.setPort(propertiesLoader.getIntProperty("rabbitmq.port", 5672));
        factory.setUsername(propertiesLoader.getProperty("rabbitmq.username", "guest"));
        factory.setPassword(propertiesLoader.getProperty("rabbitmq.password", "guest"));
        this.rawQueueName = propertiesLoader.getProperty("rabbitmq.raw.queue.name", "raw_price_updates_queue");
        this.dlqName = propertiesLoader.getProperty("rabbitmq.dlq.name", "price_updates_dlq");
        this.exchangeName = propertiesLoader.getProperty("rabbitmq.exchange.name", "price_updates_exchange");
        this.rawRoutingKey = propertiesLoader.getProperty("rabbitmq.raw.routing.key", "price.update");

        try {
            this.connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(exchangeName, "topic", true);
            Map<String, Object> args = new HashMap<>();
            args.put("x-message-ttl", propertiesLoader.getIntProperty("rabbitmq.dlq.ttl.hours", 12) * 3600 * 1000);
            args.put("x-dead-letter-exchange", exchangeName);
            args.put("x-dead-letter-routing-key", "price.update.dlq");
            channel.queueDeclare(rawQueueName, true, false, false, args);
            channel.queueBind(rawQueueName, exchangeName, rawRoutingKey);
            log.info("RabbitMQ raw queue '{}' bound to exchange '{}' with routing key '{}'", rawQueueName, exchangeName, rawRoutingKey);
            channel.queueDeclare(dlqName, true, false, false, null);
            channel.queueBind(dlqName, exchangeName, "price.update.dlq");
            log.info("RabbitMQ DLQ '{}' bound to exchange '{}' with routing key 'price.update.dlq'", dlqName, exchangeName);
            channel.close();
        } catch (IOException | TimeoutException e) {
            log.error("Failed to connect to RabbitMQ or declare queues/exchange", e);
            throw new RuntimeException("Failed to initialize RabbitMQ", e);
        }
    }

    public void close() {
        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
                log.info("RabbitMQ connection closed");
            }
        } catch (IOException e) {
            log.error("Error closing RabbitMQ connection", e);
        }
    }
}