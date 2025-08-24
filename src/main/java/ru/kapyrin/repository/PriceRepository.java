package ru.kapyrin.repository;

import ru.kapyrin.exception.PriceUpdateException;
import ru.kapyrin.model.PriceUpdate;
import ru.kapyrin.model.ProductAggregatesData;

import java.sql.Connection;
import java.util.Optional;
import java.util.function.Function;

public interface PriceRepository {

    <T> T executeInTransaction(Function<Connection, T> task) throws PriceUpdateException;

    void upsertProduct(Connection connection, Long productId, String productName) throws PriceUpdateException;

    Double getOldPriceForVendorProduct(Connection connection, Long productId, String manufacturerName) throws PriceUpdateException;

    void upsertPrice(Connection connection, PriceUpdate update) throws PriceUpdateException;

    Optional<ProductAggregatesData> getAggregatesData(Connection connection, Long productId) throws PriceUpdateException;

    Double updateAggregatesAtomically(Connection connection, long productId, double initialAvgPrice, double deltaSum, long deltaCount) throws PriceUpdateException;

    Double getStoredAveragePrice(Long productId) throws PriceUpdateException;
}