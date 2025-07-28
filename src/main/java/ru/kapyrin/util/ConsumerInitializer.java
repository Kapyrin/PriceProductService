package ru.kapyrin.util;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.PropertiesLoader;
import ru.kapyrin.config.RabbitMQConfig;
import ru.kapyrin.service.PriceCalculationService;
import ru.kapyrin.service.PriceUpdateValidator;
import ru.kapyrin.task.PriceUpdateConsumerTask;

import java.util.List;
import java.util.concurrent.ExecutorService;

@Slf4j
@RequiredArgsConstructor
public class ConsumerInitializer {
    private final PropertiesLoader propertiesLoader;
    private final RabbitMQConfig rabbitMQConfig;
    private final ExecutorService validationExecutor;
    private final ExecutorService dbExecutor;
    private final PriceCalculationService priceCalculationService;
    private final PriceUpdateValidator priceUpdateValidator;
    private final List<Thread> consumerThreads;

    public void initialize() {
        int minConsumerCount = propertiesLoader.getIntProperty("rabbitmq.consumers.min", 2);
        for (int i = 0; i < minConsumerCount; i++) {
            PriceUpdateConsumerTask consumerTask = new PriceUpdateConsumerTask(
                    rabbitMQConfig, validationExecutor, dbExecutor, priceCalculationService, priceUpdateValidator);
            Thread consumerThread = new Thread(consumerTask, "PriceUpdateConsumer-" + i);
            consumerThreads.add(consumerThread);
            consumerThread.start();
            log.info("PriceUpdateConsumerTask {} started", i);
        }
    }

    public int getMinConsumerCount() {
        return propertiesLoader.getIntProperty("rabbitmq.consumers.min", 2);
    }

    public int getMaxConsumerCount() {
        return propertiesLoader.getIntProperty("rabbitmq.consumers.max", 10);
    }

    public int getQueueSizeThreshold() {
        return propertiesLoader.getIntProperty("rabbitmq.queue.size.threshold", 1000);
    }
}