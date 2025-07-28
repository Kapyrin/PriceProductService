package ru.kapyrin.util;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.RabbitMQConfig;
import ru.kapyrin.config.RedisConfig;

@Slf4j
@RequiredArgsConstructor
public class ConnectionMonitor {
    private final RabbitMQConfig rabbitMQConfig;
    private final RedisConfig redisConfig;

    public void registerMetrics() {
        Gauge.builder("rabbitmq_connection_status", () -> rabbitMQConfig.getConnection().isOpen() ? 1.0 : 0.0)
                .description("Status of RabbitMQ connection (1.0 = connected, 0.0 = disconnected)")
                .register(Metrics.globalRegistry);
        Gauge.builder("redis_connection_status", () -> {
                    try (var jedis = redisConfig.getJedisPool().getResource()) {
                        return jedis.ping().equals("PONG") ? 1.0 : 0.0;
                    } catch (Exception e) {
                        log.error("Failed to ping Redis: {}", e.getMessage());
                        return 0.0;
                    }
                }).description("Status of Redis connection (1.0 = connected, 0.0 = disconnected)")
                .register(Metrics.globalRegistry);
        log.info("Connection metrics registered for RabbitMQ and Redis");
    }
}