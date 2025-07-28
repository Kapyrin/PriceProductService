package ru.kapyrin.config.impl;

import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.PropertiesLoader;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class FacadePropertiesLoader implements PropertiesLoader {

    private final List<PropertiesLoader> loaders;

    public FacadePropertiesLoader(PropertiesLoader... loaders) {
        this.loaders = Arrays.asList(loaders);
        log.info("CompositePropertiesLoader initialized with {} loaders.", this.loaders.size());
    }

    @Override
    public String getProperty(String key) {
        for (PropertiesLoader loader : loaders) {
            try {
                String value = loader.getProperty(key);
                if (value != null) {
                    log.debug("Property '{}' found by loader: {}", key, loader.getClass().getSimpleName());
                    return value;
                }
            } catch (Exception e) {
                log.warn("Error getting property '{}' from loader {}: {}", key, loader.getClass().getSimpleName(), e.getMessage());
            }
        }
        log.debug("Property '{}' not found in any configured loader.", key);
        return null;
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        String value = getProperty(key);
        return (value != null) ? value : defaultValue;
    }

}
