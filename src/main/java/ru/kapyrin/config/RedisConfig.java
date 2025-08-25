package ru.kapyrin.config;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

@Slf4j
@Getter
public class RedisConfig {
    private JedisPool jedisPool;
    private final int cacheExpireSeconds;
    @Setter
    private boolean isRedisAvailable;

    public RedisConfig(PropertiesLoader propertiesLoader) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(propertiesLoader.getIntProperty("redis.pool.max.total", 128));
        poolConfig.setMaxIdle(propertiesLoader.getIntProperty("redis.pool.max.idle", 128));
        poolConfig.setMinIdle(propertiesLoader.getIntProperty("redis.pool.min.idle", 16));
        poolConfig.setTestOnBorrow(propertiesLoader.getBooleanProperty("redis.pool.test.on.borrow", true));
        poolConfig.setTestOnReturn(propertiesLoader.getBooleanProperty("redis.pool.test.on.return", true));
        poolConfig.setTestWhileIdle(propertiesLoader.getBooleanProperty("redis.pool.test.while.idle", true));
        poolConfig.setMaxWait(Duration.ofMillis(propertiesLoader.getLongProperty("redis.pool.max.wait.ms", 5000L)));
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(propertiesLoader.getLongProperty("redis.pool.eviction.interval.ms", 30000L)));
        long expireSeconds = Duration.ofMinutes(propertiesLoader.getIntProperty("redis.cache.expire.minutes", 10)).getSeconds();
        this.cacheExpireSeconds = (int) Math.min(Integer.MAX_VALUE, expireSeconds);
        this.jedisPool = new JedisPool(
                poolConfig,
                propertiesLoader.getProperty("redis.host", "localhost"),
                propertiesLoader.getIntProperty("redis.port", 6379)
        );
        this.isRedisAvailable = false;
    }

    public void close() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
            log.info("Redis JedisPool closed");
        }
    }
}