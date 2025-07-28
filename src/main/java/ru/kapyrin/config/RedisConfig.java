package ru.kapyrin.config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Slf4j
@Getter
public class RedisConfig {
    private final JedisPool jedisPool;
    private final int cacheExpireMinutes;
    private final long maxCacheSize;

    public RedisConfig(PropertiesLoader propertiesLoader) {
        String redisHost = propertiesLoader.getProperty("redis.host", "localhost");
        int redisPort = propertiesLoader.getIntProperty("redis.port", 6379);
        this.cacheExpireMinutes = propertiesLoader.getIntProperty("redis.cache.expire.minutes", 10);
        this.maxCacheSize = propertiesLoader.getLongProperty("redis.max.cache.size", 1_000_000);

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);

        this.jedisPool = new JedisPool(poolConfig, redisHost, redisPort);
        log.info("Redis JedisPool initialized for {}:{}", redisHost, redisPort);
        log.info("Ensure Redis is configured with maxmemory={} bytes and maxmemory-policy=allkeys-lru",
                maxCacheSize * 8);
    }

    public void close() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
            log.info("Redis JedisPool closed");
        }
    }
}