package ru.kapyrin.task;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.PropertiesLoader;
import ru.kapyrin.config.RabbitMQConfig;
import ru.kapyrin.exception.PriceUpdateException;
import ru.kapyrin.model.PriceUpdate;
import ru.kapyrin.service.PriceCalculationService;
import ru.kapyrin.service.PriceUpdateValidator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@RequiredArgsConstructor
public class PriceUpdateConsumerTask implements Runnable{
    private final RabbitMQConfig rabbitMQConfig;
    private final ExecutorService validationExecutor;
    private final ExecutorService dbExecutor;
    private final PriceCalculationService priceCalculationService;
    private final PriceUpdateValidator validator;
    private final PropertiesLoader propertiesLoader;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Timer validationTimer = Metrics.timer("price_update_validation_time");
    private final Timer dbTimer = Metrics.timer("price_update_db_time");
    private final Counter processedMessages = Metrics.counter("rabbitmq_messages_processed");
    private final Counter errorMessages = Metrics.counter("rabbitmq_messages_errors");
    private final Counter invalidUpdates = Metrics.counter("rabbitmq_invalid_updates_total");
    private final Counter updatesSucceeded = Metrics.counter("price_updates_succeeded_total");
    private final Counter updatesFailed = Metrics.counter("price_updates_failed_total");
    private final Counter dbRetriesTotal = Metrics.counter("db_retries_total");
    private final int maxRetries;
    private final int prefetch;
    private volatile Channel channel;
    private volatile String consumerTag;
    private final ExecutorService ackExecutor = Executors.newSingleThreadExecutor();


    public PriceUpdateConsumerTask(
            RabbitMQConfig rabbitMQConfig,
            ExecutorService validationExecutor,
            ExecutorService dbExecutor,
            PriceCalculationService priceCalculationService,
            PriceUpdateValidator validator,
            PropertiesLoader propertiesLoader) {
        this.rabbitMQConfig = rabbitMQConfig;
        this.validationExecutor = validationExecutor;
        this.dbExecutor = dbExecutor;
        this.priceCalculationService = priceCalculationService;
        this.validator = validator;
        this.propertiesLoader = propertiesLoader;
        this.maxRetries = Math.max(1, propertiesLoader.getIntProperty("db.retry.max.attempts", 3));
        this.prefetch = Math.max(1, propertiesLoader.getIntProperty("rabbitmq.prefetch", 50));
        log.info("PriceUpdateConsumerTask initialized for queue '{}', prefetch={}, maxRetries={}", rabbitMQConfig.getRawQueueName(), prefetch, maxRetries);
    }

    @Override
    public void run() {
        log.info("PriceUpdateConsumerTask started, prefetch={}, maxRetries={}", prefetch, maxRetries);
        configureConsumer();
    }

    private void configureConsumer() {
        try {
            channel = rabbitMQConfig.getConnection().createChannel();
            channel.basicQos(prefetch);
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                this.consumerTag = consumerTag;
                final long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                final byte[] body = delivery.getBody();
                validationExecutor.submit(() -> processMessage(deliveryTag, body));
            };
            channel.basicConsume(rabbitMQConfig.getRawQueueName(), false, deliverCallback, tag -> {});
        } catch (IOException e) {
            log.error("Failed to start RabbitMQ consumer for queue '{}': {}", rabbitMQConfig.getRawQueueName(), e.getMessage());
            throw new RuntimeException("Failed to start RabbitMQ consumer", e);
        }
    }

    private void processMessage(long deliveryTag, byte[] body) {
        Timer.Sample sample = Timer.start();
        try {
            String jsonBody = new String(body, StandardCharsets.UTF_8);
            List<PriceUpdate> updates = objectMapper.readValue(jsonBody, new TypeReference<>() {});
            if (updates == null || updates.isEmpty()) {
                log.warn("Empty or null price updates, rejecting, deliveryTag={}", deliveryTag);
                errorMessages.increment();
                ackExecutor.submit(() -> basicRejectNoRequeue(deliveryTag));
                return;
            }

            for (PriceUpdate update : updates) {
                try {
                    validator.validatePriceUpdate(update);
                    processDbWithRetry(update);
                } catch (PriceUpdateException e) {
                    log.warn("Validation failed for productId={}: {}", update.productId(), e.getMessage());
                    invalidUpdates.increment();
                    submitOnAckExecutor(() -> publishUpdateToDlq(update));
                } catch (Exception ex) {
                    log.error("DB processing failed for productId={}: {}", update.productId(), ex.getMessage());
                    updatesFailed.increment();
                    submitOnAckExecutor(() -> publishUpdateToDlq(update));
                }
            }

            submitOnAckExecutor(() -> {
                try {
                    processedMessages.increment();
                    channel.basicAck(deliveryTag, false);
                } catch (IOException ioe) {
                    log.error("Failed to ack message, deliveryTag={}: {}", deliveryTag, ioe.getMessage());
                }
            });

        } catch (IOException e) {
            log.error("Failed to deserialize message, deliveryTag={}: {}", deliveryTag, e.getMessage());
            errorMessages.increment();
            ackExecutor.submit(() -> basicRejectNoRequeue(deliveryTag));
        } finally {
            sample.stop(validationTimer);
        }
    }

    private void processDbWithRetry(PriceUpdate update) {
        Timer.Sample dbSample = Timer.start();
        int attempt = 0;
        long backoffMs = 300L;
        while (true) {
            attempt++;
            try {
                CompletableFuture.runAsync(() -> priceCalculationService.calculateAndPersistAveragePrice(update), dbExecutor).join();
                dbSample.stop(dbTimer);
                updatesSucceeded.increment();
                return;
            } catch (Exception e) {
                if (attempt >= maxRetries) {
                    log.error("DB failed after {} attempts, product_id={}: {}", attempt, update.productId(), e.getMessage());
                    updatesFailed.increment();
                    throw new PriceUpdateException("DB operation failed after max retries", e);
                }
                dbRetriesTotal.increment();
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new PriceUpdateException("Interrupted during backoff", ie);
                }
                backoffMs = Math.min(backoffMs * 2, 5_000L);
            }
        }
    }

    private void publishUpdateToDlq(PriceUpdate update) {
        try {
            byte[] payload = objectMapper.writeValueAsBytes(update);
            channel.basicPublish("", rabbitMQConfig.getDlqName(), null, payload);
        } catch (Exception e) {
            log.error("Failed to publish item to DLQ, product_id={}: {}", update.productId(), e.getMessage());
        }
    }

    private void basicRejectNoRequeue(long deliveryTag) {
        try {
            channel.basicReject(deliveryTag, false);
        } catch (IOException e) {
            log.error("Failed to reject message, deliveryTag={}: {}", deliveryTag, e.getMessage());
        }
    }

    private void submitOnAckExecutor(Runnable r) {
        try {
            ackExecutor.submit(() -> {
                try {
                    r.run();
                } catch (Exception ex) {
                    log.error("Channel operation failed: {}", ex.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("Ack executor rejected task: {}", e.getMessage());
        }
    }

    public void shutdown() {
        log.info("Shutting down PriceUpdateConsumerTask");
        try {
            if (consumerTag != null && channel != null && channel.isOpen()) {
                channel.basicCancel(consumerTag);
            }
        } catch (Exception e) {
            log.warn("basicCancel failed: {}", e.getMessage());
        }
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        } catch (Exception e) {
            log.warn("Channel close failed: {}", e.getMessage());
        }
        ackExecutor.shutdownNow();
    }
}