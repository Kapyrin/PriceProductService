package ru.kapyrin.exception;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ApiErrorHandlers {

    public static void handleBadRequest(RoutingContext rc) {
        Throwable failure = rc.failure();
        String errorMessage = "Bad Request: " + (failure != null ? failure.getMessage() : "Invalid input.");
        log.warn("Handling 400 Bad Request for path {}: {}", rc.request().path(), errorMessage);

        rc.response()
                .putHeader("content-type", "application/json")
                .setStatusCode(400)
                .end(new JsonObject().put("status", "error").put("message", errorMessage).encode());
    }

    public static void handleNotFound(RoutingContext rc) {
        Throwable failure = rc.failure();
        String errorMessage = "Not Found: " + (failure != null ? failure.getMessage() : "Resource not found.");
        log.info("Handling 404 Not Found for path {}: {}", rc.request().path(), errorMessage);

        rc.response()
                .putHeader("content-type", "application/json")
                .setStatusCode(404)
                .end(new JsonObject().put("status", "error").put("message", errorMessage).encode());
    }

    public static void handleInternalServerError(RoutingContext rc) {
        Throwable failure = rc.failure();
        String errorMessage = "Internal Server Error: " + (failure != null ? failure.getMessage() : "An unexpected error occurred.");
        log.error("Handling 500 Internal Server Error for path {}: {}", rc.request().path(), errorMessage, failure);

        rc.response()
                .putHeader("content-type", "application/json")
                .setStatusCode(500)
                .end(new JsonObject().put("status", "error").put("message", errorMessage).encode());
    }
}