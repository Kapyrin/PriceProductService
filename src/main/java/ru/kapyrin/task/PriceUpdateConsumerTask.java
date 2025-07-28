package ru.kapyrin.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.RabbitMQConfig;
import ru.kapyrin.exception.PriceUpdateException;
import ru.kapyrin.model.PriceUpdate;
import ru.kapyrin.service.PriceCalculationService;
import ru.kapyrin.service.PriceUpdateValidator;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

@Slf4j
@RequiredArgsConstructor
public class PriceUpdateConsumerTask implements Runnable {
    private static final int MAX_RETRIES = 3;

    private final RabbitMQConfig rabbitMQConfig;
    private final ExecutorService validationExecutor;
    private final ExecutorService dbExecutor;
    private final PriceCalculationService priceCalculationService;
    private final PriceUpdateValidator validator;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Timer validationTimer;
    private final Timer dbTimer;
    private final Counter processedMessages;
    private final Counter errorMessages;

    public PriceUpdateConsumerTask(
            RabbitMQConfig rabbitMQConfig,
            ExecutorService validationExecutor,
            ExecutorService dbExecutor,
            PriceCalculationService priceCalculationService,
            PriceUpdateValidator validator) {
        this.rabbitMQConfig = rabbitMQConfig;
        this.validationExecutor = validationExecutor;
        this.dbExecutor = dbExecutor;
        this.priceCalculationService = priceCalculationService;
        this.validator = validator;
        this.validationTimer = Metrics.timer("price_update_validation_time");
        this.dbTimer = Metrics.timer("price_update_db_time");
        this.processedMessages = Metrics.counter("rabbitmq_messages_processed");
        this.errorMessages = Metrics.counter("rabbitmq_messages_errors");
        Gauge.builder("rabbitmq_queue_size", () -> {
            try (Channel channel = rabbitMQConfig.getConnection().createChannel()) {
                return (double) channel.messageCount(rabbitMQConfig.getRawQueueName());
            } catch (IOException | TimeoutException e) {
                log.error("Failed to get queue size for {}: {}", rabbitMQConfig.getRawQueueName(), e.getMessage());
                return 0.0;
            }
        }).register(Metrics.globalRegistry);
        log.info("PriceUpdateConsumerTask initialized for queue '{}'", rabbitMQConfig.getRawQueueName());
    }

    @Override
    public void run() {
        log.info("PriceUpdateConsumerTask started, waiting for messages...");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            long deliveryTag = delivery.getEnvelope().getDeliveryTag();
            log.debug("Received message from RabbitMQ: {}", message);

            validationExecutor.submit(() -> {
                Timer.Sample sample = Timer.start();
                try (Channel channel = rabbitMQConfig.getConnection().createChannel()) {
                    List<PriceUpdate> updates = objectMapper.readValue(message,
                            objectMapper.getTypeFactory().constructCollectionType(List.class, PriceUpdate.class));
                    if (updates == null || updates.isEmpty()) {
                        log.warn("Received empty or null price updates, rejecting to DLQ");
                        errorMessages.increment();
                        rejectToDlq(channel, deliveryTag, message);
                        return;
                    }

                    updates.forEach(update -> {
                        try {
                            validator.validatePriceUpdate(update);
                            dbExecutor.submit(() -> {
                                Timer.Sample dbSample = Timer.start();
                                int retryCount = 0;
                                while (retryCount < MAX_RETRIES) {
                                    try {
                                        priceCalculationService.calculateAndPersistAveragePrice(update);
                                        dbSample.stop(dbTimer);
                                        log.debug("DbExecutor: Successfully processed update for product_id={}", update.productId());
                                        break;
                                    } catch (PriceUpdateException e) {
                                        retryCount++;
                                        if (retryCount == MAX_RETRIES) {
                                            log.error("Failed to process DB update for product_id={} after {} retries: {}",
                                                    update.productId(), MAX_RETRIES, e.getMessage());
                                            errorMessages.increment();
                                            try (Channel dlqChannel = rabbitMQConfig.getConnection().createChannel()) {
                                                dlqChannel.basicPublish("", rabbitMQConfig.getDlqName(), null,
                                                        objectMapper.writeValueAsBytes(update));
                                                log.debug("Sent PriceUpdate for product_id={} to DLQ", update.productId());
                                            } catch (IOException | TimeoutException dlqEx) {
                                                log.error("Failed to send to DLQ for product_id={}: {}",
                                                        update.productId(), dlqEx.getMessage());
                                            }
                                        } else {
                                            try {
                                                Thread.sleep(1000 * retryCount);
                                            } catch (InterruptedException ie) {
                                                Thread.currentThread().interrupt();
                                            }
                                        }
                                    }
                                }
                            });
                            log.info("Validated price update for product_id={}, manufacturer={}",
                                    update.productId(), update.manufacturerName());
                        } catch (PriceUpdateException e) {
                            log.error("Failed to validate price update for product_id={}: {}",
                                    update.productId(), e.getMessage());
                            errorMessages.increment();
                            try (Channel dlqChannel = rabbitMQConfig.getConnection().createChannel()) {
                                dlqChannel.basicPublish("", rabbitMQConfig.getDlqName(), null,
                                        objectMapper.writeValueAsBytes(update));
                                log.debug("Sent PriceUpdate for product_id={} to DLQ", update.productId());
                            } catch (IOException | TimeoutException dlqEx) {
                                log.error("Failed to send to DLQ for product_id={}: {}",
                                        update.productId(), dlqEx.getMessage());
                            }
                        }
                    });
                    processedMessages.increment();
                    channel.basicAck(deliveryTag, false);
                    sample.stop(validationTimer);
                } catch (JsonProcessingException e) {
                    log.error("Failed to deserialize RabbitMQ message to JSON: {}", e.getMessage());
                    errorMessages.increment();
                    try (Channel channel = rabbitMQConfig.getConnection().createChannel()) {
                        rejectToDlq(channel, deliveryTag, message);
                    } catch (IOException | TimeoutException ex) {
                        log.error("Failed to create channel for DLQ: {}", ex.getMessage());
                    }
                } catch (IOException | TimeoutException e) {
                    log.error("Failed to process RabbitMQ message: {}", e.getMessage());
                    errorMessages.increment();
                    try (Channel channel = rabbitMQConfig.getConnection().createChannel()) {
                        rejectToDlq(channel, deliveryTag, message);
                    } catch (IOException | TimeoutException ex) {
                        log.error("Failed to create channel for DLQ: {}", ex.getMessage());
                    }
                }
            });
        };

        try (Channel channel = rabbitMQConfig.getConnection().createChannel()) {
            channel.basicConsume(rabbitMQConfig.getRawQueueName(), false, deliverCallback, consumerTag -> {});
        } catch (IOException | TimeoutException e) {
            log.error("Failed to start RabbitMQ consumer for queue '{}': {}", rabbitMQConfig.getRawQueueName(), e.getMessage());
            throw new RuntimeException("Failed to start RabbitMQ consumer", e);
        }
    }

    private void rejectToDlq(Channel channel, long deliveryTag, String message) {
        try {
            channel.basicReject(deliveryTag, false);
            channel.basicPublish("", rabbitMQConfig.getDlqName(), null, message.getBytes("UTF-8"));
            log.debug("Message rejected and sent to DLQ for deliveryTag={}", deliveryTag);
        } catch (IOException e) {
            log.error("Failed to reject message to DLQ for deliveryTag={}: {}", deliveryTag, e.getMessage());
        }
    }

    public void shutdown() {
        log.info("Shutting down PriceUpdateConsumerTask");
    }
}