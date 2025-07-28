package ru.kapyrin.util;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.RabbitMQConfig;
import ru.kapyrin.service.PriceCalculationService;
import ru.kapyrin.service.PriceUpdateValidator;
import ru.kapyrin.task.PriceUpdateConsumerTask;

import java.util.List;
import java.util.concurrent.ExecutorService;
import io.micrometer.core.instrument.Metrics;

@Slf4j
@RequiredArgsConstructor
public class ConsumerScaler {
    private final List<Thread> consumerThreads;
    private final int minConsumerCount;
    private final int maxConsumerCount;
    private final int queueSizeThreshold;
    private final RabbitMQConfig rabbitMQConfig;
    private final ExecutorService validationExecutor;
    private final ExecutorService dbExecutor;
    private final PriceCalculationService priceCalculationService;
    private final PriceUpdateValidator priceUpdateValidator;

    public void scale() {
        try {
            double queueSize = Metrics.globalRegistry.find("rabbitmq_queue_size").gauge().value();
            int desiredConsumers = Math.min(Math.max(minConsumerCount, (int) (queueSize / queueSizeThreshold) + 1), maxConsumerCount);
            while (consumerThreads.size() < desiredConsumers) {
                PriceUpdateConsumerTask consumerTask = new PriceUpdateConsumerTask(
                        rabbitMQConfig, validationExecutor, dbExecutor, priceCalculationService, priceUpdateValidator);
                Thread consumerThread = new Thread(consumerTask, "PriceUpdateConsumer-" + consumerThreads.size());
                consumerThreads.add(consumerThread);
                consumerThread.start();
                log.info("Added new consumer thread, total consumers: {}", consumerThreads.size());
            }
            while (consumerThreads.size() > desiredConsumers && consumerThreads.size() > minConsumerCount) {
                Thread thread = consumerThreads.remove(consumerThreads.size() - 1);
                thread.interrupt();
                log.info("Removed consumer thread, total consumers: {}", consumerThreads.size());
            }
        } catch (Exception e) {
            log.error("Scaling failed: {}", e.getMessage());
        }
    }
}