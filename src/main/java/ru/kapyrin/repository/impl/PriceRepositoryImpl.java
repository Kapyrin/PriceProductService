package ru.kapyrin.repository.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.exception.PriceUpdateException;
import ru.kapyrin.model.PriceUpdate;
import ru.kapyrin.model.ProductAggregatesData;
import ru.kapyrin.repository.PriceRepository;
import ru.kapyrin.repository.SqlQueries;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Optional;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public class PriceRepositoryImpl implements PriceRepository {
    private final DataSource dataSource;

    @Override
    public <T> T executeInTransaction(Function<Connection, T> task) throws PriceUpdateException {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            Savepoint savepoint = connection.setSavepoint();
            try {
                T result = task.apply(connection);
                connection.commit();
                return result;
            } catch (Exception e) {
                connection.rollback(savepoint);
                throw new PriceUpdateException("Transaction failed, rolling back", e);
            }
        } catch (SQLException e) {
            throw new PriceUpdateException("Failed to get connection for transaction", e);
        }
    }

    @Override
    public void upsertProduct(Connection connection, Long productId, String productName) throws PriceUpdateException {
        try (PreparedStatement ps = connection.prepareStatement(SqlQueries.UPSERT_PRODUCT)) {
            ps.setLong(1, productId);
            ps.setString(2, productName);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new PriceUpdateException("Failed to upsert product", e);
        }
    }

    @Override
    public Double getOldPriceForVendorProduct(Connection connection, Long productId, String manufacturerName) throws PriceUpdateException {
        try (PreparedStatement ps = connection.prepareStatement(SqlQueries.SELECT_OLD_PRICE_FOR_VENDOR_PRODUCT)) {
            ps.setLong(1, productId);
            ps.setString(2, manufacturerName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getDouble("price") : null;
            }
        } catch (SQLException e) {
            throw new PriceUpdateException("Failed to get old price for vendor product", e);
        }
    }

    @Override
    public void upsertPrice(Connection connection, PriceUpdate update) throws PriceUpdateException {
        try (PreparedStatement ps = connection.prepareStatement(SqlQueries.UPSERT_PRICE)) {
            ps.setLong(1, update.productId());
            ps.setString(2, update.manufacturerName());
            ps.setDouble(3, update.price());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new PriceUpdateException("Failed to upsert price", e);
        }
    }

    @Override
    public Optional<ProductAggregatesData> getAggregatesData(Connection connection, Long productId) throws PriceUpdateException {
        try (PreparedStatement ps = connection.prepareStatement(SqlQueries.SELECT_AGGREGATES_DATA)) {
            ps.setLong(1, productId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new ProductAggregatesData(
                            rs.getLong("product_id"),
                            rs.getDouble("avg_price"),
                            rs.getDouble("total_sum_prices"),
                            rs.getLong("offer_count")
                    ));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new PriceUpdateException("Failed to get aggregates data", e);
        }
    }

    @Override
    public Double updateAggregatesAtomically(Connection connection, long productId, double initialAvgPrice, double deltaSum, long deltaCount) throws PriceUpdateException {
        try (PreparedStatement ps = connection.prepareStatement(SqlQueries.ATOMIC_UPDATE_AGGREGATES)) {
            ps.setLong(1, productId);
            ps.setDouble(2, initialAvgPrice);
            ps.setDouble(3, deltaSum);
            ps.setLong(4, deltaCount);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getDouble("avg_price");
                }
                throw new PriceUpdateException("Atomic update did not return average price.");
            }
        } catch (SQLException e) {
            throw new PriceUpdateException("Failed to execute atomic aggregate update", e);
        }
    }

    @Override
    public Double getStoredAveragePrice(Long productId) throws PriceUpdateException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(SqlQueries.GET_STORED_AVG_PRICE)) {
            ps.setLong(1, productId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getDouble("avg_price") : null;
            }
        } catch (SQLException e) {
            throw new PriceUpdateException("Failed to get stored average price", e);
        }
    }
}