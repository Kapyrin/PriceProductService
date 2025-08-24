package ru.kapyrin.service.impl;

import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import ru.kapyrin.config.RedisConfig;
import ru.kapyrin.repository.PriceRepository;
import ru.kapyrin.service.MetricsService;
import ru.kapyrin.service.PriceAverageCalculator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

@Slf4j
@RequiredArgsConstructor
public class PriceAverageCalculatorImpl implements PriceAverageCalculator {
    private final PriceRepository priceRepository;
    private final RedisConfig redisConfig;
    private final MetricsService metricsService;
    private final ExecutorService dbExecutor;
    private final int cacheExpireSeconds;

    public PriceAverageCalculatorImpl(PriceRepository priceRepository, RedisConfig redisConfig,
                                      MetricsService metricsService, ExecutorService dbExecutor) {
        this.priceRepository = priceRepository;
        this.redisConfig = redisConfig;
        this.metricsService = metricsService;
        this.dbExecutor = dbExecutor;
        this.cacheExpireSeconds = redisConfig.getCacheExpireSeconds();
    }

    @Override
    public CompletableFuture<Double> getAveragePriceAsync(Long productId) {
        metricsService.recordGetRequest();
        if (productId == null || productId <= 0) {
            log.warn("Invalid product ID: {}", productId);
            metricsService.recordGetError();
            return CompletableFuture.failedFuture(new IllegalArgumentException("Invalid product ID: " + productId));
        }
        return CompletableFuture.supplyAsync(() -> {
            Timer.Sample sample = metricsService.startGetTimer();
            try {
                if (redisConfig.isRedisAvailable()) {
                    try (Jedis jedis = redisConfig.getJedisPool().getResource()) {
                        String cachedPrice = jedis.get("avg_price:" + productId);
                        if (cachedPrice != null) {
                            return Double.parseDouble(cachedPrice);
                        }
                    } catch (JedisConnectionException e) {
                        log.error("Redis connection lost for product_id={}: {}", productId, e.getMessage());
                        redisConfig.setRedisAvailable(false);
                    } catch (Exception e) {
                        log.error("Error accessing Redis cache for product_id={}: {}", productId, e.getMessage());
                    }
                }
                Double price = priceRepository.getStoredAveragePrice(productId);
                if (price != null) {
                    updateAveragePriceCaches(productId, price);
                    return price;
                }
                throw new IllegalStateException("Product not found or no average price");
            } catch (Exception e) {
                log.error("Error getting average price for product_id={}: {}", productId, e.getMessage());
                metricsService.recordGetError();
                throw new IllegalStateException("Product not found or no average price", e);
            } finally {
                metricsService.stopGetTimer(sample);
            }
        }, dbExecutor);
    }

    @Override
    public void updateAveragePriceCaches(Long productId, Double newAveragePrice) {
        if (redisConfig.isRedisAvailable() && redisConfig.getJedisPool() != null) {
            try (Jedis jedis = redisConfig.getJedisPool().getResource()) {
                if (newAveragePrice != null) {
                    jedis.setex("avg_price:" + productId, cacheExpireSeconds, String.valueOf(newAveragePrice));
                    log.debug("Updated Redis cache for product_id={}: {}", productId, newAveragePrice);
                } else {
                    jedis.del("avg_price:" + productId);
                    log.debug("Invalidated Redis cache for product_id={}", productId);
                }
            } catch (JedisConnectionException e) {
                log.error("Redis connection lost for product_id={}: {}", productId, e.getMessage());
                redisConfig.setRedisAvailable(false);
            } catch (Exception e) {
                log.error("Error updating/invalidating Redis cache for product_id={}: {}", productId, e.getMessage());
            }
        }
    }
}