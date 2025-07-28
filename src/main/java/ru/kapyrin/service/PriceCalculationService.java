package ru.kapyrin.service;

import ru.kapyrin.exception.PriceUpdateException;
import ru.kapyrin.model.PriceUpdate;

public interface PriceCalculationService {
    Double calculateAndPersistAveragePrice(PriceUpdate priceUpdate) throws PriceUpdateException;
}
