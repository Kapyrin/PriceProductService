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
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMaxWait(Duration.ofSeconds(5));
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30));
        this.cacheExpireSeconds = propertiesLoader.getIntProperty("redis.cache.expire.minutes", 10) * 60;
        this.jedisPool = new JedisPool(poolConfig, propertiesLoader.getProperty("redis.host", "localhost"),
                propertiesLoader.getIntProperty("redis.port", 6379));
        this.isRedisAvailable = false;
    }

    public void close() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
            log.info("Redis JedisPool closed");
        }
    }
}