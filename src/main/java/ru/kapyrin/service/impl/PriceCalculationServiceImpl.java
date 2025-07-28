package ru.kapyrin.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.exception.PriceUpdateException;
import ru.kapyrin.model.PriceUpdate;
import ru.kapyrin.model.ProductAggregates;
import ru.kapyrin.repository.PriceRepository;
import ru.kapyrin.service.PriceAverageCalculator;
import ru.kapyrin.service.PriceCalculationService;

@Slf4j
@RequiredArgsConstructor
public class PriceCalculationServiceImpl implements PriceCalculationService {

    private final PriceRepository repository;
    private final PriceAverageCalculator priceAverageCalculator;

    @Override
    public Double calculateAndPersistAveragePrice(PriceUpdate priceUpdate) throws PriceUpdateException {
        Double newCalculatedAverage = repository.executeInTransaction(connection -> {
            repository.upsertProduct(connection, priceUpdate.productId(), "Unknown Product Name");
            Double oldPriceForVendor = repository.getOldPriceForVendorProduct(connection, priceUpdate.productId(), priceUpdate.manufacturerName());
            repository.upsertPrice(connection, priceUpdate);

            ProductAggregates currentAggregates = repository.getAggregates(connection, priceUpdate.productId());
            Double currentTotalSum = currentAggregates.totalSumPrices();
            Long currentOfferCount = currentAggregates.offerCount();

            Double updatedTotalSum = currentTotalSum;
            Long updatedOfferCount = currentOfferCount;

            if (oldPriceForVendor != null) {
                updatedTotalSum = currentTotalSum - oldPriceForVendor + priceUpdate.price();
            } else {
                updatedTotalSum = currentTotalSum + priceUpdate.price();
                updatedOfferCount = currentOfferCount + 1;
            }

            Double calculatedAverage = (updatedOfferCount > 0) ? updatedTotalSum / updatedOfferCount : null;
            log.debug("Calculated new aggregates for product_id={}: sum={}, count={}, average={}", priceUpdate.productId(), updatedTotalSum, updatedOfferCount, calculatedAverage);

            repository.upsertAggregates(connection, priceUpdate.productId(), calculatedAverage, updatedTotalSum, updatedOfferCount);

            return calculatedAverage;
        });

        priceAverageCalculator.updateAveragePriceCaches(priceUpdate.productId(), newCalculatedAverage);
        log.debug("PriceCalculationService: Updated Redis cache for product_id={} after DB commit.", priceUpdate.productId());

        return newCalculatedAverage;
    }
}