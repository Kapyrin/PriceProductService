package ru.kapyrin;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.DatabaseConfig;
import ru.kapyrin.config.PropertiesLoader;
import ru.kapyrin.config.RabbitMQConfig;
import ru.kapyrin.config.RedisConfig;
import ru.kapyrin.config.impl.EnvironmentPropertiesLoader;
import ru.kapyrin.config.impl.FacadePropertiesLoader;
import ru.kapyrin.config.impl.FilePropertiesLoader;
import ru.kapyrin.controller.PriceApiVerticle;
import ru.kapyrin.repository.PriceRepository;
import ru.kapyrin.repository.impl.PriceRepositoryImpl;
import ru.kapyrin.service.PriceAverageCalculator;
import ru.kapyrin.service.PriceCalculationService;
import ru.kapyrin.service.PriceUpdateValidator;
import ru.kapyrin.service.RawPriceUpdatePublisher;
import ru.kapyrin.service.impl.PriceAverageCalculatorImpl;
import ru.kapyrin.service.impl.PriceCalculationServiceImpl;
import ru.kapyrin.service.impl.PriceUpdateValidatorImpl;
import ru.kapyrin.service.impl.RawPriceUpdatePublisherImpl;
import ru.kapyrin.util.ApplicationShutdownHandler;
import ru.kapyrin.util.ConsumerInitializer;
import ru.kapyrin.util.ConsumerScaler;
import ru.kapyrin.util.DlqProcessor;
import ru.kapyrin.util.ConnectionMonitor;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Main {
    public static void main(String[] args) {
        PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Metrics.globalRegistry.add(meterRegistry);

        PropertiesLoader fileLoader = new FilePropertiesLoader("application.properties");
        PropertiesLoader envLoader = new EnvironmentPropertiesLoader();
        PropertiesLoader propertiesLoader = new FacadePropertiesLoader(envLoader, fileLoader);

        DatabaseConfig databaseConfig = new DatabaseConfig(propertiesLoader);
        DataSource dataSource = databaseConfig.createDataSource();
        log.info("HikariCP DataSource initialized");

        RabbitMQConfig rabbitMQConfig = new RabbitMQConfig(propertiesLoader);
        log.info("RabbitMQConfig initialized");

        RedisConfig redisConfig = new RedisConfig(propertiesLoader);
        log.info("RedisConfig initialized");

        PriceRepository priceRepository = new PriceRepositoryImpl(dataSource);
        log.info("PriceRepository initialized");

        PriceAverageCalculator priceAverageCalculator = new PriceAverageCalculatorImpl(priceRepository, redisConfig);
        log.info("PriceAverageCalculator initialized");

        PriceCalculationService priceCalculationService = new PriceCalculationServiceImpl(priceRepository, priceAverageCalculator);
        log.info("PriceCalculationService initialized");

        PriceUpdateValidator priceUpdateValidator = new PriceUpdateValidatorImpl();
        log.info("PriceUpdateValidator initialized");

        RawPriceUpdatePublisher rawPriceUpdatePublisher = new RawPriceUpdatePublisherImpl(rabbitMQConfig);
        log.info("RawPriceUpdatePublisher initialized");

        ExecutorService validationExecutor = Executors.newWorkStealingPool();
        log.info("Validation/Deserialization Executor (WorkStealingPool) initialized");

        ExecutorService dbExecutorVirtual = Executors.newVirtualThreadPerTaskExecutor();
        log.info("DB Executor (Virtual Threads) initialized");

        ConnectionMonitor connectionMonitor = new ConnectionMonitor(rabbitMQConfig, redisConfig);
        connectionMonitor.registerMetrics();

        List<Thread> consumerThreads = new ArrayList<>();
        ConsumerInitializer consumerInitializer = new ConsumerInitializer(
                propertiesLoader, rabbitMQConfig, validationExecutor, dbExecutorVirtual,
                priceCalculationService, priceUpdateValidator, consumerThreads);
        consumerInitializer.initialize();

        ScheduledExecutorService scalingExecutor = Executors.newSingleThreadScheduledExecutor();
        ConsumerScaler scaler = new ConsumerScaler(
                consumerThreads,
                consumerInitializer.getMinConsumerCount(),
                consumerInitializer.getMaxConsumerCount(),
                consumerInitializer.getQueueSizeThreshold(),
                rabbitMQConfig, validationExecutor, dbExecutorVirtual,
                priceCalculationService, priceUpdateValidator);
        scalingExecutor.scheduleAtFixedRate(scaler::scale, 0, 60, TimeUnit.SECONDS);

        ExecutorService dlqProcessor = Executors.newSingleThreadExecutor();
        DlqProcessor dlqProcessorTask = new DlqProcessor(rabbitMQConfig);
        dlqProcessor.submit(dlqProcessorTask::process);

        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new PriceApiVerticle(rawPriceUpdatePublisher, priceAverageCalculator, propertiesLoader, dbExecutorVirtual, meterRegistry))
                .onSuccess(id -> log.info("PriceApiVerticle deployed with ID: {}", id))
                .onFailure(err -> log.error("Failed to deploy PriceApiVerticle: {}", err.getMessage()));

        ApplicationShutdownHandler shutdownHandler = new ApplicationShutdownHandler(
                rawPriceUpdatePublisher, consumerThreads, dlqProcessor, rabbitMQConfig, redisConfig,
                dataSource, validationExecutor, dbExecutorVirtual, scalingExecutor);
        Runtime.getRuntime().addShutdownHook(new Thread(shutdownHandler::shutdown));
    }
}