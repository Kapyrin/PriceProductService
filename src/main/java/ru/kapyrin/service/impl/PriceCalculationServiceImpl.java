package ru.kapyrin.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.exception.PriceUpdateException;
import ru.kapyrin.model.PriceUpdate;
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

            final double deltaSum;
            final long deltaCount;

            if (oldPriceForVendor != null) {
                deltaSum = priceUpdate.price() - oldPriceForVendor;
                deltaCount = 0L;
            } else {
                deltaSum = priceUpdate.price();
                deltaCount = 1L;
            }

            double initialAvg = (deltaCount > 0) ? deltaSum : 0.0;
            return repository.updateAggregatesAtomically(connection, priceUpdate.productId(), initialAvg, deltaSum, deltaCount);
        });

        priceAverageCalculator.updateAveragePriceCaches(priceUpdate.productId(), newCalculatedAverage);
        log.debug("PriceCalculationService: Updated Redis cache for product_id={} with new average={}", priceUpdate.productId(), newCalculatedAverage);

        return newCalculatedAverage;
    }
}