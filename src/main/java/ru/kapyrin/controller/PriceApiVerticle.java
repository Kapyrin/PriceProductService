package ru.kapyrin.controller;

import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.PropertiesLoader;
import ru.kapyrin.exception.ApiErrorHandlers;
import ru.kapyrin.service.MetricsService;
import ru.kapyrin.service.PriceAverageCalculator;
import ru.kapyrin.service.RawPriceUpdatePublisher;

@Slf4j
@RequiredArgsConstructor
public class PriceApiVerticle extends AbstractVerticle {
    private final RawPriceUpdatePublisher rawPriceUpdatePublisher;
    private final PriceAverageCalculator priceAverageCalculator;
    private final PropertiesLoader propertiesLoader;
    private final MetricsService metricsService;
    private final PrometheusMeterRegistry meterRegistry;

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
                .handler(BodyHandler.create().setBodyLimit(propertiesLoader.getLongProperty("max.body.size", 3_145_728L)))
                .blockingHandler(rc -> {
                    Timer.Sample sample = metricsService.startPostTimer();
                    try {
                        String jsonBody = rc.body().asString();
                        log.debug("Received POST /price-updates, size={} bytes", jsonBody.length());
                        rawPriceUpdatePublisher.publishRawPriceUpdate(jsonBody);

                        rc.response()
                                .putHeader("content-type", "application/json")
                                .setStatusCode(202)
                                .end("{\"status\":\"ok\", \"message\":\"Price updates submitted for processing\"}");
                    } catch (Exception e) {
                        rc.fail(e);
                    } finally {
                        metricsService.stopPostTimer(sample);
                    }
                }, false);

        router.get("/average-price/:productId")
                .handler(rc -> {
                    Timer.Sample sample = metricsService.startGetTimer();
                    try {
                        long productId = Long.parseLong(rc.pathParam("productId"));
                        log.debug("Received GET /average-price/{}", productId);
                        priceAverageCalculator.getAveragePriceAsync(productId)
                                .whenComplete((avgPrice, e) -> {
                                    metricsService.stopGetTimer(sample);
                                    if (e == null) {
                                        JsonObject jsonResponse = new JsonObject()
                                                .put("product_id", productId)
                                                .put("average_price", String.format("%.2f", avgPrice));
                                        rc.response()
                                                .putHeader("content-type", "application/json")
                                                .setStatusCode(200)
                                                .end(jsonResponse.encode());
                                    } else {
                                        Throwable cause = e.getCause() != null ? e.getCause() : e;
                                        if (cause instanceof IllegalStateException && cause.getMessage().contains("Product not found")) {
                                            rc.fail(404, cause);
                                        } else {
                                            rc.fail(cause);
                                        }
                                    }
                                });
                    } catch (NumberFormatException e) {
                        log.warn("Invalid product ID format: {}", rc.pathParam("productId"));
                        metricsService.stopGetTimer(sample);
                        rc.fail(400, e);
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