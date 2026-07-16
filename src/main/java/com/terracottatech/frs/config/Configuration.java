/*
 * Copyright IBM Corp. 2024, 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author tim
 */
public class Configuration {
  public static final String USER_PROPERTIES_FILE = "frs.properties";

  private final EnumMap<FrsProperty, Object> configuration;
  private final File dbHome;

  private Configuration(File home, Map<FrsProperty, Object> properties) {
    this.configuration = new EnumMap<FrsProperty, Object>(properties);
    this.dbHome = home;
  }

  public static Configuration getConfiguration(File directory, Properties overrides) {
    Map<FrsProperty, Object> configuration = new EnumMap<FrsProperty, Object>(FrsProperty.class);
    configuration.putAll(extractConfiguration(getUserProperties(directory), true, false));
    configuration.putAll(extractConfiguration(overrides, true, false));
    configuration.putAll(extractConfiguration(System.getProperties(), false, true));
    return new Configuration(directory, configuration);
  }

  public static Configuration getConfiguration(File directory) {
    return getConfiguration(directory, new Properties());
  }

  public Object getConfigurationValue(FrsProperty property) {
    Object value = configuration.get(property);
    return value == null ? property.defaultValue() : value;
  }
  
  public String getString(FrsProperty property) {
    return (String) getConfigurationValue(property);
  }

  public Integer getInt(FrsProperty property) {
    return (Integer) getConfigurationValue(property);
  }

  public Long getLong(FrsProperty property) {
    return (Long) getConfigurationValue(property);
  }

  public Double getDouble(FrsProperty property) {
    return (Double) getConfigurationValue(property);
  }

  public Float getFloat(FrsProperty property) {
    return (Float) getConfigurationValue(property);
  }

  public Boolean getBoolean(FrsProperty property) {
    return (Boolean) getConfigurationValue(property);
  }
  
  public File getDBHome() {
    return dbHome;
  }

  @Override
  public String toString() {
    return configuration.toString();
  }

  private static Properties getUserProperties(File directory) {
    Properties properties = new Properties();
    File useFile = new File(directory, USER_PROPERTIES_FILE);
    if (useFile.exists()) {
      FileInputStream fis;
      try {
        fis = new FileInputStream(useFile);
        try {
          properties.load(fis);
        } finally {
          fis.close();
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to read user frs configuration.", e);
      }
    }
    return properties;
  }
  
  private static Map<FrsProperty, Object> extractConfiguration(Properties properties, boolean shortNames, boolean tolerateMismatches) {
    Map<FrsProperty, Object> configuration = new EnumMap<FrsProperty, Object>(FrsProperty.class);
    if (tolerateMismatches) {
      for (FrsProperty property : FrsProperty.values()) {
        String propertyName = shortNames ? property.shortName() : property.property();
        String value = (String) properties.get(propertyName);
        if (value != null) {
          configuration.put(property, property.convert(value));
        }
      }
    } else {
      Map<Object, Object> map = new HashMap<Object, Object>(properties);
      for (FrsProperty property : FrsProperty.values()) {
        String propertyName = shortNames ? property.shortName() : property.property();
        String value = (String) map.remove(propertyName);
        if (value != null) {
          configuration.put(property, property.convert(value));
        }
      }
      if (!map.isEmpty()) {
        throw new IllegalArgumentException("Unrecognized properties: " + map);
      }
    }
    return configuration;
  }
}
