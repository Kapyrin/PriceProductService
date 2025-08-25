package ru.kapyrin.exception;

public class DatabaseInitializeException extends RuntimeException {
    public DatabaseInitializeException(String message) {
        super(message);
    }

    public DatabaseInitializeException(String message, Throwable cause) {
    }
}
