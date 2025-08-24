package ru.kapyrin.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;

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
        config.setMaximumPoolSize(propertiesLoader.getIntProperty("db.pool.size", 20));
        config.setMinimumIdle(propertiesLoader.getIntProperty("db.pool.min.idle", 5));
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
        try {
            Flyway flyway = Flyway.configure(ClassLoader.getSystemClassLoader())
                    .dataSource(dataSource)
                    .locations("classpath:db/migration")
                    .baselineOnMigrate(true)
                    .load();
            flyway.migrate();
            log.info("Database schema initialized successfully with Flyway.");
        } catch (Exception e) {
            log.error("Failed to initialize database schema with Flyway: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to initialize database schema", e);
        }
    }
}