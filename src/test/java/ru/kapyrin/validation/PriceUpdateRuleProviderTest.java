package ru.kapyrin.validation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.kapyrin.model.PriceUpdate;

import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

class PriceUpdateRuleProviderTest {

    private PriceUpdateRuleProvider ruleProvider;

    @BeforeEach
    void setUp() {
        ruleProvider = new PriceUpdateRuleProvider();
    }

    @Test
    void getValidationRules_shouldReturnDefaultRules() {
        List<PriceUpdateRuleProvider.ValidationRule> rules = ruleProvider.getValidationRules();
        assertEquals(3, rules.size(), "По умолчанию должно быть 3 правила валидации");
        assertTrue(rules.stream().anyMatch(rule -> rule.message().equals("Product ID must be positive")));
        assertTrue(rules.stream().anyMatch(rule -> rule.message().equals("Manufacturer name cannot be empty")));
        assertTrue(rules.stream().anyMatch(rule -> rule.message().equals("Price cannot be negative")));
    }

    @Test
    void addValidationRule_shouldAddNewRule() {
        String newMessage = "Новое тестовое правило";
        Predicate<PriceUpdate> newPredicate = update -> true;

        ruleProvider.addValidationRule(newPredicate, newMessage);
        List<PriceUpdateRuleProvider.ValidationRule> rules = ruleProvider.getValidationRules();

        assertEquals(4, rules.size(), "Количество правил должно увеличиться до 4");
        assertTrue(rules.stream().anyMatch(rule -> rule.message().equals(newMessage)), "Новое правило должно быть в списке");
    }

    @Test
    void removeValidationRule_shouldRemoveRule() {
        String messageToRemove = "Price cannot be negative";

        ruleProvider.removeValidationRule(messageToRemove);
        List<PriceUpdateRuleProvider.ValidationRule> rules = ruleProvider.getValidationRules();

        assertEquals(2, rules.size(), "Количество правил должно уменьшиться до 2");
        assertFalse(rules.stream().anyMatch(rule -> rule.message().equals(messageToRemove)), "Удаленное правило не должно быть в списке");
    }
}