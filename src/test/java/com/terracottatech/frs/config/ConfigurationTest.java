package com.terracottatech.frs.config;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author tim
 */
public class ConfigurationTest {
  @Test
  public void testOverrides() throws Exception {
    File directory = new File("testFileOverrides");
    if (directory.exists()) {
      FileUtils.deleteDirectory(directory);
    }
    assert directory.mkdirs();

    Properties properties = new Properties();
    properties.setProperty("foo", "baz");
    properties.setProperty("compactor.policy", "bogus123");
    properties.setProperty("abc", "123");

    System.setProperty("com.tc.frs.compactor.lsnGap.minLoad", "1.00");

    FileOutputStream fos = new FileOutputStream(new File(directory, "frs.properties"));
    try {
      properties.store(fos, null);
    } finally {
      fos.close();
    }

    Configuration configuration = Configuration.getConfiguration(directory);

    assertThat(configuration.getString("foo"), is("baz"));
    assertThat(configuration.getString("compactor.policy"), is("bogus123"));
    assertThat(configuration.getInt("abc"), is(123));
    assertThat(configuration.getDouble("compactor.lsnGap.minLoad"), is(1.00));

    Properties overrides = new Properties();
    overrides.setProperty("foo", "bar");
    overrides.setProperty("gel", "banana");

    configuration = Configuration.getConfiguration(directory, overrides);

    assertThat(configuration.getString("foo"), is("bar"));
    assertThat(configuration.getString("compactor.policy"), is("bogus123"));
    assertThat(configuration.getInt("abc"), is(123));
    assertThat(configuration.getDouble("compactor.lsnGap.minLoad"), is(1.00));
    assertThat(configuration.getString("gel"), is("banana"));
  }
}
