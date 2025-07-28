package ru.kapyrin.config.impl;

import lombok.extern.slf4j.Slf4j;
import ru.kapyrin.config.PropertiesLoader;
import ru.kapyrin.exception.PropertyReadException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

@Slf4j
public class FilePropertiesLoader implements PropertiesLoader {

    private final Properties properties;

    public FilePropertiesLoader() {
        String customPath = System.getProperty("application.properties.path");
        if (customPath != null && !customPath.isEmpty()) {
            this.properties = loadPropertiesFromFileInternal(customPath);
        } else {
            this.properties = loadPropertiesFromResourceInternal("application.properties");
        }
    }

    public FilePropertiesLoader(String resourceFileName) {
        this.properties = loadPropertiesFromResourceInternal(resourceFileName);
    }

    private Properties loadPropertiesFromResourceInternal(String resourceFileName) {
        Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(resourceFileName)) {
            if (input == null) {
                log.error("Sorry, unable to find {}", resourceFileName);
                throw new PropertyReadException("Unable to find " + resourceFileName);
            }
            props.load(input);
            log.info("Successfully loaded properties from {}", resourceFileName);
            return props;
        } catch (IOException ex) {
            log.error("Error loading properties from {}", resourceFileName, ex);
            throw new PropertyReadException("Error loading properties from " + resourceFileName, ex);
        }
    }

    private Properties loadPropertiesFromFileInternal(String filePath) {
        Properties props = new Properties();
        Path path = Paths.get(filePath);
        try (InputStream input = Files.newInputStream(path)) {
            props.load(input);
            log.info("Successfully loaded properties from file: {}", filePath);
            return props;
        } catch (IOException ex) {
            log.error("Error loading properties from file: {}", filePath, ex);
            throw new PropertyReadException("Error loading properties from file: " + filePath, ex);
        }
    }

    @Override
    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
}
