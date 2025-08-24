package ru.kapyrin.util;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.PropertiesLoader;
import ru.kapyrin.config.RabbitMQConfig;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

@Slf4j
@RequiredArgsConstructor
public class DlqProcessor {
    private final RabbitMQConfig rabbitMQConfig;
    private final PropertiesLoader propertiesLoader;
    private final Counter dlqMessages = Metrics.counter("dlq_messages_total");
    private final CountDownLatch latch = new CountDownLatch(1);

    public void process() {
        Channel channel = null;
        String consumerTag = null;
        try {
            channel = rabbitMQConfig.getConnection().createChannel();
            int prefetch = propertiesLoader.getIntProperty("rabbitmq.dlq.prefetch", 10);
            channel.basicQos(prefetch);
            DeliverCallback callback = (tag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                log.warn("Received message in DLQ: {}", message);
                dlqMessages.increment();
            };
            consumerTag = channel.basicConsume(rabbitMQConfig.getDlqName(), true, callback, tag -> {});
            log.info("DLQ processor started for queue={}, prefetch={}", rabbitMQConfig.getDlqName(), prefetch);
            latch.await();
        } catch (Exception e) {
            log.error("Failed to process DLQ: {}", e.getMessage());
        } finally {
            try {
                if (channel != null && channel.isOpen() && consumerTag != null) {
                    channel.basicCancel(consumerTag);
                }
            } catch (Exception ignore) {}
            try {
                if (channel != null && channel.isOpen()) {
                    channel.close();
                }
            } catch (Exception ignore) {}
            log.info("DLQ processor stopped");
        }
    }

    public void shutdown() {
        latch.countDown();
    }
}