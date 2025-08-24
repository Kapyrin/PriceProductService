package ru.kapyrin.service;

import io.micrometer.core.instrument.Timer;

public interface MetricsService {
    void recordPostRequest();
    void recordGetRequest();
    void recordPostError();
    void recordGetError();
    Timer.Sample startPostTimer();
    Timer.Sample startGetTimer();
    void stopPostTimer(Timer.Sample sample);
    void stopGetTimer(Timer.Sample sample);
    void recordBatchSize(int size);
}