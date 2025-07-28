package ru.kapyrin.exception;

public class PropertyReadException extends RuntimeException {
    public PropertyReadException(String message) {
        super(message);
    }
    public PropertyReadException(String message, Throwable cause) {
    }
}
