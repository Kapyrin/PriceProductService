package ru.kapyrin.util;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.RabbitMQConfig;
import ru.kapyrin.config.RedisConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

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

    public void checkRedisConnection() {
        if (redisConfig.getJedisPool() == null) {
            redisConfig.setRedisAvailable(false);
            return;
        }
        try (Jedis jedis = redisConfig.getJedisPool().getResource()) {
            jedis.ping();
            if (!redisConfig.isRedisAvailable()) {
                log.info("Redis connection restored");
                redisConfig.setRedisAvailable(true);
            }
        } catch (JedisConnectionException e) {
            if (redisConfig.isRedisAvailable()) {
                log.error("Redis connection lost: {}", e.getMessage());
                redisConfig.setRedisAvailable(false);
            }
        } catch (Exception e) {
            log.error("Error checking Redis connection: {}", e.getMessage());
        }
    }
}