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

  public static Configuration getConfiguration(File directory) {
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
    return new Configuration(directory, properties);
  }

  private static String prefixKey(String key) {
    return SYSTEM_PROPERTY_PREFIX + key;
  }

  public String getString(String key, String defaultValue) {
    return System.getProperty(prefixKey(key), properties.getProperty(key, defaultValue));
  }

  public int getInt(String key, int defaultValue) {
    return Integer.parseInt(getString(key, Integer.toString(defaultValue)));
  }

  public long getLong(String key, long defaultValue) {
    return Long.parseLong(getString(key, Long.toString(defaultValue)));
  }

  public double getDouble(String key, double defaultValue) {
    return Double.parseDouble(getString(key, Double.toString(defaultValue)));
  }

  public float getFloat(String key, float defaultValue) {
    return Float.parseFloat(getString(key, Float.toString(defaultValue)));
  }

  public boolean getBoolean(String key, boolean defaultValue) {
    return Boolean.parseBoolean(getString(key, Boolean.toString(defaultValue)));
  }

  public String getString(String key) {
    return System.getProperty(prefixKey(key), properties.getProperty(key));
  }

  public Integer getInt(String key) {
    String val = getString(key);
    return val == null ? null : Integer.parseInt(val);
  }

  public Long getLong(String key) {
    String val = getString(key);
    return val == null ? null : Long.parseLong(val);
  }

  public Double getDouble(String key) {
    String val = getString(key);
    return val == null ? null : Double.parseDouble(val);
  }

  public Float getFloat(String key) {
    String val = getString(key);
    return val == null ? null : Float.parseFloat(val);
  }

  public Boolean getBoolean(String key) {
    String val = getString(key);
    return val == null ? null : Boolean.parseBoolean(val);
  }
  
  public File getDBHome() {
      return dbhome;
  }

  @Override
  public String toString() {
    return properties.toString();
  }
}
