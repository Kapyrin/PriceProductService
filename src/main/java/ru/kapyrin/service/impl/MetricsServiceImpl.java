package ru.kapyrin.service.impl;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.service.MetricsService;

@Slf4j
@RequiredArgsConstructor
public class MetricsServiceImpl implements MetricsService {
    private final PrometheusMeterRegistry meterRegistry;
    private final Counter postRequests = Metrics.counter("http_post_price_updates_requests");
    private final Counter getRequests = Metrics.counter("http_get_average_price_requests");
    private final Counter postErrors = Metrics.counter("http_post_price_updates_errors");
    private final Counter getErrors = Metrics.counter("http_get_average_price_errors");
    private final Timer postTimer = Metrics.timer("http_post_price_updates_time");
    private final Timer getTimer = Metrics.timer("http_get_average_price_time");
    private final Counter batchSize = Metrics.counter("http_post_price_updates_batch_size");

    @Override
    public void recordPostRequest() {
        postRequests.increment();
    }

    @Override
    public void recordGetRequest() {
        getRequests.increment();
    }

    @Override
    public void recordPostError() {
        postErrors.increment();
    }

    @Override
    public void recordGetError() {
        getErrors.increment();
    }

    @Override
    public Timer.Sample startPostTimer() {
        return Timer.start(meterRegistry);
    }

    @Override
    public Timer.Sample startGetTimer() {
        return Timer.start(meterRegistry);
    }

    @Override
    public void stopPostTimer(Timer.Sample sample) {
        sample.stop(postTimer);
    }

    @Override
    public void stopGetTimer(Timer.Sample sample) {
        sample.stop(getTimer);
    }

    @Override
    public void recordBatchSize(int size) {
        batchSize.increment(size);
    }
}
