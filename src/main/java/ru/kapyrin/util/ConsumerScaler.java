package ru.kapyrin.util;

import lombok.RequiredArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.PropertiesLoader;
import ru.kapyrin.config.RabbitMQConfig;
import ru.kapyrin.service.PriceCalculationService;
import ru.kapyrin.service.PriceUpdateValidator;
import ru.kapyrin.task.PriceUpdateConsumerTask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import io.micrometer.core.instrument.Metrics;

@Slf4j
@RequiredArgsConstructor
public class ConsumerScaler {
    private final List<Thread> consumerThreads;
    @Getter
    private final List<PriceUpdateConsumerTask> consumerTasks = new ArrayList<>();
    private final  int minConsumerCount;
    private final  int maxConsumerCount;
    private final  int queueSizeThreshold;
    private final  RabbitMQConfig rabbitMQConfig;
    private final  ExecutorService validationExecutor;
    private final  ExecutorService dbExecutor;
    private final  PriceCalculationService priceCalculationService;
    private final  PriceUpdateValidator priceUpdateValidator;
    private final  PropertiesLoader propertiesLoader;

    public void scale() {
        try {
            Double queueSizeObj = Metrics.globalRegistry.find("rabbitmq_queue_size").gauge().value();
            double queueSize = queueSizeObj != null ? queueSizeObj : 0.0;
            int desiredConsumers = Math.min(Math.max(minConsumerCount, (int) (queueSize / queueSizeThreshold) + 1), maxConsumerCount);
            while (consumerThreads.size() < desiredConsumers) {
                PriceUpdateConsumerTask consumerTask = new PriceUpdateConsumerTask(
                        rabbitMQConfig, validationExecutor, dbExecutor, priceCalculationService, priceUpdateValidator, propertiesLoader);
                Thread consumerThread = new Thread(consumerTask, "PriceUpdateConsumer-" + consumerThreads.size());
                consumerThreads.add(consumerThread);
                consumerTasks.add(consumerTask);
                consumerThread.start();
                log.info("Added new consumer thread, total consumers: {}", consumerThreads.size());
            }
            while (consumerThreads.size() > desiredConsumers && consumerThreads.size() > minConsumerCount) {
                Thread thread = consumerThreads.remove(consumerThreads.size() - 1);
                PriceUpdateConsumerTask task = consumerTasks.remove(consumerTasks.size() - 1);
                task.shutdown();
                thread.interrupt();
                log.info("Removed consumer thread, total consumers: {}", consumerThreads.size());
            }
        } catch (Exception e) {
            log.error("Scaling failed: {}", e.getMessage());
        }
    }
}