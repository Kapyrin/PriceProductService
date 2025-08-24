package ru.kapyrin.service.impl;

import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.exception.PriceUpdateException;
import ru.kapyrin.model.PriceUpdate;
import ru.kapyrin.service.PriceUpdateValidator;
import ru.kapyrin.validation.PriceUpdateRuleProvider;

import java.util.Optional;

@Slf4j
public class PriceUpdateValidatorImpl implements PriceUpdateValidator {
    private final PriceUpdateRuleProvider ruleProvider;

    public PriceUpdateValidatorImpl() {
        this.ruleProvider = new PriceUpdateRuleProvider();
        log.info("PriceUpdateValidatorImpl initialized with default PriceUpdateRuleProvider.");
    }

    @Override
    public void validatePriceUpdate(PriceUpdate priceUpdate) throws PriceUpdateException {

        Optional<String> firstError = ruleProvider.getValidationRules().stream()
                .filter(rule -> rule.predicate().test(priceUpdate))
                .map(PriceUpdateRuleProvider.ValidationRule::message)
                .findFirst();

        if (firstError.isPresent()) {
            String errorMessage = firstError.get();
            log.warn("Validation failed for price update {}: {}", priceUpdate, errorMessage);
            throw new PriceUpdateException(errorMessage);
        }
    }
}