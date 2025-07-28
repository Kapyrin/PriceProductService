package ru.kapyrin.repository.impl;

import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.exception.PriceUpdateException;
import ru.kapyrin.model.PriceUpdate;
import ru.kapyrin.model.ProductAggregates;
import ru.kapyrin.model.ProductAggregatesData;
import ru.kapyrin.repository.PriceRepository;
import ru.kapyrin.repository.SqlQueries;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Function;

@Slf4j
public class PriceRepositoryImpl implements PriceRepository {

    private final DataSource dataSource;

    public PriceRepositoryImpl(DataSource dataSource) {
        this.dataSource = dataSource;
        log.info("PriceRepositoryImpl initialized with DataSource.");
    }

    @Override
    public <T> T executeInTransaction(Function<Connection, T> task) throws PriceUpdateException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);

            T result = task.apply(connection);

            connection.commit();
            log.debug("Transaction committed successfully.");
            return result;
        } catch (SQLException e) {
            if (connection != null) {
                try {
                    connection.rollback();
                    log.warn("Transaction rolled back due to SQL error.", e);
                } catch (SQLException rollbackEx) {
                    log.error("Failed to rollback transaction:", rollbackEx);
                }
            }
            log.error("Failed to execute transaction: {}", e.getMessage(), e);
            throw new PriceUpdateException("Failed to execute transaction: " + e.getMessage(), e);
        } finally {
            try {
                if (connection != null) {
                    connection.setAutoCommit(true);
                    connection.close();
                }
            } catch (SQLException closeEx) {
                log.error("Error closing database connection:", closeEx);
            }
        }
    }

    @Override
    public void upsertProduct(Connection connection, Long productId, String productName) throws PriceUpdateException {
        try (PreparedStatement productStmt = connection.prepareStatement(SqlQueries.UPSERT_PRODUCT)) {
            productStmt.setLong(1, productId);
            productStmt.setString(2, productName);
            productStmt.executeUpdate();
            log.debug("Ensured product existence for product_id={}", productId);
        } catch (SQLException e) {
            log.error("Failed to upsert product {}: {}", productId, e.getMessage(), e);
            throw new PriceUpdateException("Failed to upsert product: " + e.getMessage(), e);
        }
    }

    @Override
    public Double getOldPriceForVendorProduct(Connection connection, Long productId, String manufacturerName) throws PriceUpdateException {
        try (PreparedStatement ps = connection.prepareStatement(SqlQueries.SELECT_OLD_PRICE_FOR_VENDOR_PRODUCT)) {
            ps.setLong(1, productId);
            ps.setString(2, manufacturerName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    double oldPrice = rs.getDouble("price");
                    return rs.wasNull() ? null : oldPrice;
                }
                return null;
            }
        } catch (SQLException e) {
            log.error("Failed to get old price for product_id={} by manufacturer={}: {}", productId, manufacturerName, e.getMessage(), e);
            throw new PriceUpdateException("Failed to get old price: " + e.getMessage(), e);
        }
    }

    @Override
    public void upsertPrice(Connection connection, PriceUpdate update) throws PriceUpdateException {
        try (PreparedStatement priceStmt = connection.prepareStatement(SqlQueries.UPSERT_PRICE)) {
            priceStmt.setLong(1, update.productId());
            priceStmt.setString(2, update.manufacturerName());
            priceStmt.setDouble(3, update.price());
            priceStmt.executeUpdate();
            log.debug("Upserted new price {} for product_id={} by manufacturer={}", update.price(), update.productId(), update.manufacturerName());
        } catch (SQLException e) {
            log.error("Failed to upsert price for product_id={} by manufacturer={}: {}", update.productId(), update.manufacturerName(), e.getMessage(), e);
            throw new PriceUpdateException("Failed to upsert price: " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<ProductAggregatesData> getAggregatesData(Connection connection, Long productId) throws PriceUpdateException {
        try (PreparedStatement ps = connection.prepareStatement(SqlQueries.SELECT_AGGREGATES_DATA)) {
            ps.setLong(1, productId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Double avgPrice = rs.getDouble("avg_price");
                    Double totalSum = rs.getDouble("total_sum_prices");
                    Long offerCount = rs.getLong("offer_count");

                    if (rs.wasNull() && avgPrice == 0.0) {
                        return Optional.of(ProductAggregatesData.empty(productId));
                    }
                    return Optional.of(new ProductAggregatesData(productId, avgPrice, totalSum, offerCount));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            log.error("Failed to get aggregates data for product_id={}: {}", productId, e.getMessage(), e);
            throw new PriceUpdateException("Failed to get aggregates data: " + e.getMessage(), e);
        }
    }

    @Override
    public void upsertAggregates(Connection connection, Long productId, Double avgPrice, Double totalSumPrices, Long offerCount) throws PriceUpdateException {
        try (PreparedStatement ps = connection.prepareStatement(SqlQueries.UPSERT_AVG_PRICE_WITH_AGGREGATES)) {
            ps.setLong(1, productId);
            if (avgPrice == null) {
                ps.setNull(2, java.sql.Types.DOUBLE);
            } else {
                ps.setDouble(2, avgPrice);
            }
            ps.setDouble(3, totalSumPrices);
            ps.setLong(4, offerCount);
            ps.executeUpdate();
            log.debug("Updated product_avg_price for product_id={} with new aggregates. Avg={}, Sum={}, Count={}", productId, avgPrice, totalSumPrices, offerCount);
        } catch (SQLException e) {
            log.error("Failed to upsert aggregates for product_id={}: {}", productId, e.getMessage(), e);
            throw new PriceUpdateException("Failed to upsert aggregates: " + e.getMessage(), e);
        }
    }

    @Override
    public ProductAggregates getAggregates(Connection connection, Long productId) throws PriceUpdateException {
        try (PreparedStatement ps = connection.prepareStatement(SqlQueries.SELECT_AGGREGATES_FROM_AVG_PRICE)) {
            ps.setLong(1, productId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    double totalSum = rs.getDouble("total_sum_prices");
                    long offerCount = rs.getLong("offer_count");
                    if (rs.wasNull() && totalSum == 0.0) { // Проверка rs.wasNull() для double, когда 0.0 может быть реальным значением
                        return ProductAggregates.empty();
                    }
                    return new ProductAggregates(totalSum, offerCount);
                }
                return ProductAggregates.empty();
            }
        } catch (SQLException e) {
            log.error("Failed to get aggregates for product_id={}: {}", productId, e.getMessage(), e);
            throw new PriceUpdateException("Failed to get aggregates: " + e.getMessage(), e);
        }
    }

    @Override
    public Double getAveragePrice(Long productId) throws PriceUpdateException {
        log.debug("PriceRepositoryImpl: Attempting to get average price for product_id={}", productId);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(SqlQueries.GET_AVERAGE_PRICE)) {
            pstmt.setLong(1, productId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    double avgPrice = rs.getDouble(1);
                    return rs.wasNull() ? null : avgPrice;
                }
                return null;
            }
        } catch (SQLException e) {
            log.error("PriceRepositoryImpl: SQL Error trying to get average price for product_id={}: {}", productId, e.getMessage(), e);
            throw new PriceUpdateException("Failed to calculate average price: " + e.getMessage(), e);
        }
    }

    @Override
    public Double getStoredAveragePrice(Long productId) throws PriceUpdateException {
        log.debug("PriceRepositoryImpl: Attempting to get stored average price for product_id={}", productId);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(SqlQueries.GET_STORED_AVG_PRICE)) {
            pstmt.setLong(1, productId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    double avgPrice = rs.getDouble(1);
                    return rs.wasNull() ? null : avgPrice;
                }
                return null;
            }
        } catch (SQLException e) {
            log.error("PriceRepositoryImpl: SQL Error trying to get stored average price for product_id={}: {}", productId, e.getMessage(), e);
            throw new PriceUpdateException("Failed to get stored average price: " + e.getMessage(), e);
        }
    }
}