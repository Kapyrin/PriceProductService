package ru.kapyrin.util;

import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.RabbitMQConfig;

@Slf4j
@RequiredArgsConstructor
public class DlqProcessor {
    private final RabbitMQConfig rabbitMQConfig;

    public void process() {
        try (Channel dlqChannel = rabbitMQConfig.getConnection().createChannel()) {
            while (!Thread.currentThread().isInterrupted()) {
                dlqChannel.basicConsume(rabbitMQConfig.getDlqName(), true, (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    log.warn("Received message in DLQ: {}", message);
                }, consumerTag -> {});
                Thread.sleep(60000);
            }
        } catch (Exception e) {
            log.error("Failed to process DLQ: {}", e.getMessage());
        }
    }
}