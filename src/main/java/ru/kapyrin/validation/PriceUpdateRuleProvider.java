package ru.kapyrin.validation;

import ru.kapyrin.model.PriceUpdate;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class PriceUpdateRuleProvider {
    private final List<ValidationRule> validationRules = new ArrayList<>();

    public PriceUpdateRuleProvider() {
        validationRules.add(new ValidationRule(
                update -> update.productId() <= 0,
                "Product ID must be positive"
        ));
        validationRules.add(new ValidationRule(
                update -> update.manufacturerName() == null || update.manufacturerName().isBlank(),
                "Manufacturer name cannot be empty"
        ));
        validationRules.add(new ValidationRule(
                update -> update.price() < 0,
                "Price cannot be negative"
        ));
    }

    public List<ValidationRule> getValidationRules() {
        return List.copyOf(validationRules);
    }

    public void addValidationRule(Predicate<PriceUpdate> predicate, String message) {
        validationRules.add(new ValidationRule(predicate, message));
    }

    public void removeValidationRule(String message) {
        validationRules.removeIf(rule -> rule.message().equals(message));
    }

    public record ValidationRule(Predicate<PriceUpdate> predicate, String message) {}
}