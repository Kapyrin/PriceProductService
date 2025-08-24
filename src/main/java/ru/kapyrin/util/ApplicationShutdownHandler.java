package ru.kapyrin.util;

import com.zaxxer.hikari.HikariDataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.RabbitMQConfig;
import ru.kapyrin.config.RedisConfig;
import ru.kapyrin.service.RawPriceUpdatePublisher;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ApplicationShutdownHandler {
    private final RawPriceUpdatePublisher rawPriceUpdatePublisher;
    private final List<Thread> consumerThreads;
    private final ExecutorService dlqProcessor;
    private final RabbitMQConfig rabbitMQConfig;
    private final RedisConfig redisConfig;
    private final DataSource dataSource;
    private final ExecutorService validationExecutor;
    private final ExecutorService dbExecutor;
    private final ScheduledExecutorService scalingExecutor;
    private final ConsumerInitializer consumerInitializer;
    private final DlqProcessor dlqProcessorTask;

    public ApplicationShutdownHandler(
            RawPriceUpdatePublisher rawPriceUpdatePublisher,
            List<Thread> consumerThreads,
            ExecutorService dlqProcessor,
            RabbitMQConfig rabbitMQConfig,
            RedisConfig redisConfig,
            DataSource dataSource,
            ExecutorService validationExecutor,
            ExecutorService dbExecutor,
            ScheduledExecutorService scalingExecutor,
            ConsumerInitializer consumerInitializer,
            DlqProcessor dlqProcessorTask) {
        this.rawPriceUpdatePublisher = rawPriceUpdatePublisher;
        this.consumerThreads = consumerThreads;
        this.dlqProcessor = dlqProcessor;
        this.rabbitMQConfig = rabbitMQConfig;
        this.redisConfig = redisConfig;
        this.dataSource = dataSource;
        this.validationExecutor = validationExecutor;
        this.dbExecutor = dbExecutor;
        this.scalingExecutor = scalingExecutor;
        this.consumerInitializer = consumerInitializer;
        this.dlqProcessorTask = dlqProcessorTask;
    }

    public void shutdown() {
        log.info("Shutting down application...");
        rawPriceUpdatePublisher.shutdown();
        consumerInitializer.shutdown();
        dlqProcessorTask.shutdown();
        consumerThreads.forEach(Thread::interrupt);
        dlqProcessor.shutdownNow();
        rabbitMQConfig.close();
        redisConfig.close();
        if (dataSource instanceof HikariDataSource hikariDataSource) {
            hikariDataSource.close();
        }
        validationExecutor.shutdownNow();
        dbExecutor.shutdownNow();
        scalingExecutor.shutdownNow();
        try {
            if (!validationExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Validation executor did not terminate in time");
            }
            if (!dbExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("DB executor did not terminate in time");
            }
            if (!scalingExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Scaling executor did not terminate in time");
            }
            if (!dlqProcessor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("DLQ processor did not terminate in time");
            }
        } catch (InterruptedException e) {
            log.error("Shutdown interrupted", e);
            Thread.currentThread().interrupt();
        }
        log.info("Application shutdown complete");
    }
}