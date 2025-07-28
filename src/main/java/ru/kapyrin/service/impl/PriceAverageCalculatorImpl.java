package ru.kapyrin.service.impl;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import ru.kapyrin.config.RedisConfig;
import ru.kapyrin.exception.PriceUpdateException;
import ru.kapyrin.repository.PriceRepository;
import ru.kapyrin.service.PriceAverageCalculator;

@Slf4j
public class PriceAverageCalculatorImpl implements PriceAverageCalculator {
    private final PriceRepository repository;
    private final JedisPool jedisPool;
    private final int cacheExpireSeconds;

    public PriceAverageCalculatorImpl(PriceRepository repository, RedisConfig redisConfig) {
        this.repository = repository;
        this.jedisPool = redisConfig.getJedisPool();
        this.cacheExpireSeconds = redisConfig.getCacheExpireMinutes() * 60;
        log.info("PriceAverageCalculatorImpl initialized with JedisPool. Cache expiration: {} seconds", cacheExpireSeconds);
        log.info("Ensure Redis is configured with maxmemory={} bytes and maxmemory-policy=allkeys-lru",
                redisConfig.getMaxCacheSize() * 8);
    }

    @Override
    public Double getAveragePrice(Long productId) {
        log.debug("PriceAverageCalculator: Attempting to get average price for product_id={}", productId);

        try (Jedis jedis = jedisPool.getResource()) {
            String cachedPriceStr = jedis.get("avg_price:" + productId);
            if (cachedPriceStr != null) {
                double cachedPrice = Double.parseDouble(cachedPriceStr);
                log.debug("PriceAverageCalculator: Found average price in Redis cache for product_id={}: {}", productId, cachedPrice);
                return cachedPrice;
            }
        } catch (Exception e) {
            log.error("Error accessing Redis cache for product_id={}: {}", productId, e.getMessage());
        }

        try {
            Double storedPrice = repository.getStoredAveragePrice(productId);
            if (storedPrice != null) {
                log.debug("PriceAverageCalculator: Found stored average price in product_avg_price for product_id={}: {}", productId, storedPrice);
                updateAveragePriceCaches(productId, storedPrice);
                return storedPrice;
            }
        } catch (PriceUpdateException e) {
            log.warn("PriceAverageCalculator: Failed to retrieve stored average price for product_id={} from DB: {}", productId, e.getMessage());
        }

        try {
            Double calculatedPrice = repository.getAveragePrice(productId);
            if (calculatedPrice != null) {
                log.debug("PriceAverageCalculator: Calculated average price from product_price for product_id={}: {}", productId, calculatedPrice);
                updateAveragePriceCaches(productId, calculatedPrice);
                return calculatedPrice;
            } else {
                log.info("PriceAverageCalculator: No average price found or calculated for product_id={}", productId);
                updateAveragePriceCaches(productId, null);
            }
        } catch (PriceUpdateException e) {
            log.error("PriceAverageCalculator: Failed to calculate average price from product_price for product_id={} due to DB error: {}", productId, e.getMessage());
        }
        return null;
    }

    @Override
    public void updateAveragePriceCaches(Long productId, Double newAveragePrice) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (newAveragePrice != null) {
                jedis.setex("avg_price:" + productId, cacheExpireSeconds, String.valueOf(newAveragePrice));
                log.debug("PriceAverageCalculator: Updated Redis cache for product_id={}: {}", productId, newAveragePrice);
            } else {
                jedis.del("avg_price:" + productId);
                log.debug("PriceAverageCalculator: Invalidated Redis cache for product_id={}", productId);
            }
        } catch (Exception e) {
            log.error("Error updating/invalidating Redis cache for product_id={}: {}", productId, e.getMessage());
        }
    }
}