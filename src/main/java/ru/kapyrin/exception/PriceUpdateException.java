package ru.kapyrin.exception;

public class PriceUpdateException extends RuntimeException {
    public PriceUpdateException(String message) {
        super(message);
    }
    public PriceUpdateException(String message, Throwable cause) {
        super(message, cause);
    }
}
