package ru.kapyrin.config.impl;

import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.PropertiesLoader;



@Slf4j
public class EnvironmentPropertiesLoader implements PropertiesLoader {

    @Override
    public String getProperty(String key) {
        String envKey = convertToEnvVarName(key);
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isEmpty()) {
            log.debug("Property '{}' loaded from environment variable '{}'", key, envKey);
            return envValue;
        }
        log.debug("Property '{}' not found in environment variable '{}'", key, envKey);
        return null;
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        String value = getProperty(key);
        return (value != null) ? value : defaultValue;
    }


    private String convertToEnvVarName(String key) {
        return key.toUpperCase().replace(".", "_");
    }
}
