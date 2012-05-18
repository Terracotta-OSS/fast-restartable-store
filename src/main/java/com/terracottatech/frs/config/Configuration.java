package com.terracottatech.frs.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author tim
 */
public class Configuration {
  private static final String DEFAULT_PROPERTIES_FILE = "/frsDefaults.properties";
  private static final String USER_PROPERITES_FILE = "frs.properties";
  private static final String SYSTEM_PROPERTY_PREFIX = "com.tc.frs.";

  private final Properties properties;
  private final File       dbhome;

  private Configuration(File home, Properties properties) {
    this.properties = properties;
    dbhome = home;
  }

  public static Configuration getConfiguration(File directory, Properties overrides) {
    Properties properties = new Properties();
    try {
      properties.load(Configuration.class.getResourceAsStream(DEFAULT_PROPERTIES_FILE));
    } catch (IOException e) {
      throw new RuntimeException("Failed to read default properties.", e);
    }

    File useFile = new File(directory, USER_PROPERITES_FILE);
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
    properties.putAll(overrides);
    return new Configuration(directory, properties);
  }

  public static Configuration getConfiguration(File directory) {
    return getConfiguration(directory, new Properties());
  }

  private static String prefixKey(String key) {
    return SYSTEM_PROPERTY_PREFIX + key;
  }

  public String getString(FrsProperty property) {
    String key = property.name();
    return System.getProperty(prefixKey(key), properties.getProperty(key));
  }

  public Integer getInt(FrsProperty property) {
    String val = getString(property);
    return val == null ? (Integer) property.defaultValue() : Integer.parseInt(val);
  }

  public Long getLong(FrsProperty property) {
    String val = getString(property);
    return val == null ? (Long) property.defaultValue() : Long.parseLong(val);
  }

  public Double getDouble(FrsProperty property) {
	if (property.type() == Double.class) {
	  String val = getString(property);
	  return val == null ?  (Double) property.defaultValue() : Double.parseDouble(val);
	} else {
	  throw new IllegalArgumentException();
	}
  }

  public Float getFloat(FrsProperty property) {
    if (property.type() == Float.class) {
      String val = getString(property);
      return val == null ?  (Float) property.defaultValue() : Float.parseFloat(val);
    } else {
      throw new IllegalArgumentException();
    }
  }

  public Boolean getBoolean(FrsProperty property) {
    if (property.type() == Boolean.class) {
      String val = getString(property);
      return val == null ?  (Boolean) property.defaultValue() : Boolean.parseBoolean(val);
    } else {
      throw new IllegalArgumentException();
    }
  }
  
  public File getDBHome() {
      return dbhome;
  }

  @Override
  public String toString() {
    return properties.toString();
  }
}
