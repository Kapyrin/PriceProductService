package ru.kapyrin.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
public class DatabaseConfig {

    private final PropertiesLoader propertiesLoader;

    public DatabaseConfig(PropertiesLoader propertiesLoader) {
        this.propertiesLoader = propertiesLoader;
    }

    public DataSource createDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(propertiesLoader.getProperty("db.url"));
        config.setUsername(propertiesLoader.getProperty("db.username"));
        config.setPassword(propertiesLoader.getProperty("db.password"));
        config.setMaximumPoolSize(propertiesLoader.getIntProperty("db.pool.size", 10));
        config.setMinimumIdle(propertiesLoader.getIntProperty("db.pool.minIdle", 5));
        config.setConnectionTimeout(propertiesLoader.getLongProperty("db.connectionTimeout", 30000));
        config.setIdleTimeout(propertiesLoader.getLongProperty("db.idleTimeout", 600000));
        config.setMaxLifetime(propertiesLoader.getLongProperty("db.maxLifetime", 1800000));
        config.setAutoCommit(true);

        log.info("Attempting to create HikariDataSource for URL: {}", config.getJdbcUrl());
        HikariDataSource dataSource = new HikariDataSource(config);
        log.info("HikariDataSource created successfully. Pool size: {}", config.getMaximumPoolSize());

        initializeDatabaseSchema(dataSource);
        return dataSource;
    }

    private void initializeDatabaseSchema(DataSource dataSource) {
        String schemaSql = """
            CREATE TABLE IF NOT EXISTS products (
                product_id BIGINT PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            );
            CREATE TABLE IF NOT EXISTS product_price (
                product_id BIGINT REFERENCES products(product_id),
                manufacturer_name VARCHAR(255) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (product_id, manufacturer_name)
            );
            CREATE TABLE IF NOT EXISTS product_avg_price (
                product_id BIGINT PRIMARY KEY REFERENCES products(product_id),
                avg_price DECIMAL(10,2),
                total_sum_prices DECIMAL(20,2) DEFAULT 0.0,
                offer_count BIGINT DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """;
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(schemaSql);
            log.info("Database schema initialized successfully.");
        } catch (SQLException e) {
            log.error("Failed to initialize database schema: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to initialize database schema", e);
        }
    }
}