package ru.kapyrin.controller;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.PropertiesLoader;
import ru.kapyrin.exception.ApiErrorHandlers;
import ru.kapyrin.service.PriceAverageCalculator;
import ru.kapyrin.service.RawPriceUpdatePublisher;

import java.util.concurrent.ExecutorService;

@Slf4j
@RequiredArgsConstructor
public class PriceApiVerticle extends AbstractVerticle {
    private final RawPriceUpdatePublisher rawPriceUpdatePublisher;
    private final PriceAverageCalculator priceAverageCalculator;
    private final PropertiesLoader propertiesLoader;
    private final ExecutorService dbExecutorVirtual;
    private final PrometheusMeterRegistry meterRegistry;
    private final Counter postRequests;
    private final Counter getRequests;
    private final Counter postErrors;
    private final Counter getErrors;
    private final Timer postTimer;
    private final Timer getTimer;

    public PriceApiVerticle(RawPriceUpdatePublisher rawPriceUpdatePublisher,
                            PriceAverageCalculator priceAverageCalculator,
                            PropertiesLoader propertiesLoader,
                            ExecutorService dbExecutorVirtual,
                            PrometheusMeterRegistry meterRegistry) {
        this.rawPriceUpdatePublisher = rawPriceUpdatePublisher;
        this.priceAverageCalculator = priceAverageCalculator;
        this.propertiesLoader = propertiesLoader;
        this.dbExecutorVirtual = dbExecutorVirtual;
        this.meterRegistry = meterRegistry;
        this.postRequests = Metrics.counter("http_post_price_updates_requests");
        this.getRequests = Metrics.counter("http_get_average_price_requests");
        this.postErrors = Metrics.counter("http_post_price_updates_errors");
        this.getErrors = Metrics.counter("http_get_average_price_errors");
        this.postTimer = Metrics.timer("http_post_price_updates_time");
        this.getTimer = Metrics.timer("http_get_average_price_time");
        log.info("PriceApiVerticle initialized");
    }

    @Override
    public void start() {
        Router router = Router.router(vertx);

        router.get("/health").handler(ctx -> {
            ctx.response().setStatusCode(200).end("{\"status\":\"UP\"}");
        });

        router.get("/metrics").handler(ctx -> {
            String metrics = meterRegistry.scrape();
            ctx.response()
                    .putHeader("content-type", "text/plain; version=0.0.4")
                    .setStatusCode(200)
                    .end(metrics);
        });

        router.errorHandler(400, ApiErrorHandlers::handleBadRequest);
        router.errorHandler(404, ApiErrorHandlers::handleNotFound);
        router.errorHandler(500, ApiErrorHandlers::handleInternalServerError);

        router.post("/price-updates")
                .handler(BodyHandler.create())
                .handler(rc -> {
                    Timer.Sample sample = Timer.start(meterRegistry);
                    postRequests.increment();
                    try {
                        if (rc.body() == null || rc.body().buffer() == null || rc.body().buffer().length() == 0) {
                            log.warn("Received POST /price-updates with empty body");
                            postErrors.increment();
                            rc.fail(400, new IllegalArgumentException("Request body cannot be empty"));
                            return;
                        }

                        String jsonBody = rc.body().asString();
                        log.debug("Received POST /price-updates with raw body: {}", jsonBody);

                        try {
                            Json.decodeValue(jsonBody);
                            rawPriceUpdatePublisher.publishRawPriceUpdate(jsonBody);
                            rc.response()
                                    .putHeader("content-type", "application/json")
                                    .setStatusCode(202)
                                    .end("{\"status\":\"ok\", \"message\":\"Price updates submitted for processing\"}");
                            sample.stop(postTimer);
                        } catch (DecodeException e) {
                            log.warn("Received POST /price-updates with invalid JSON body: {}", jsonBody, e);
                            postErrors.increment();
                            rc.fail(400, new IllegalArgumentException("Invalid JSON format: " + e.getMessage()));
                        }
                    } catch (Exception e) {
                        log.error("Error processing /price-updates: {}", e.getMessage(), e);
                        postErrors.increment();
                        rc.fail(500, new IllegalStateException("Unexpected error processing price updates: " + e.getMessage()));
                    }
                });

        router.get("/average-price/:productId")
                .handler(rc -> {
                    Timer.Sample sample = Timer.start(meterRegistry);
                    getRequests.increment();
                    String productIdStr = rc.pathParam("productId");
                    log.debug("Received GET /average-price/{}", productIdStr);
                    try {
                        long productId = Long.parseLong(productIdStr);
                        dbExecutorVirtual.submit(() -> {
                            try {
                                Double avgPrice = priceAverageCalculator.getAveragePrice(productId);
                                if (avgPrice != null) {
                                    JsonObject jsonResponse = new JsonObject()
                                            .put("product_id", productId)
                                            .put("average_price", String.format("%.2f", avgPrice));
                                    rc.response()
                                            .putHeader("content-type", "application/json")
                                            .setStatusCode(200)
                                            .end(jsonResponse.encode());
                                    sample.stop(getTimer);
                                } else {
                                    log.info("Product ID {} not found or no average price", productId);
                                    getErrors.increment();
                                    rc.fail(404, new IllegalStateException("Product not found or no average price"));
                                }
                            } catch (Exception e) {
                                log.error("Error getting average price for product_id={}: {}", productId, e.getMessage());
                                getErrors.increment();
                                rc.fail(500, new IllegalStateException("Unexpected error fetching average price: " + e.getMessage()));
                            }
                        });
                    } catch (NumberFormatException e) {
                        log.warn("Invalid product ID format: {}", productIdStr);
                        getErrors.increment();
                        rc.fail(400, new IllegalArgumentException("Invalid product ID format: " + productIdStr));
                    }
                });

        int port = propertiesLoader.getIntProperty("server.port", 8080);
        vertx.createHttpServer()
                .requestHandler(router)
                .listen(port)
                .onSuccess(server -> log.info("Vert.x HTTP server started on port {}", server.actualPort()))
                .onFailure(err -> log.error("Failed to start Vert.x HTTP server: {}", err.getMessage()));
    }

    @Override
    public void stop() {
        log.info("PriceApiVerticle stopped");
    }
}