package ru.kapyrin.service;

import ru.kapyrin.exception.PriceUpdateException;
import ru.kapyrin.model.PriceUpdate;

public interface PriceUpdateValidator {
    void validatePriceUpdate(PriceUpdate priceUpdate) throws PriceUpdateException;
}
