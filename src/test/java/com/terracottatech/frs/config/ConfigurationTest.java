package com.terracottatech.frs.config;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
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
    Assert.assertTrue(directory.mkdirs());

    Properties properties = new Properties();
    properties.setProperty(FrsProperty.COMPACTOR_POLICY.shortName(), "bogus123");
    FileOutputStream fos = new FileOutputStream(new File(directory, "frs.properties"));
    try {
      properties.store(fos, null);
    } finally {
      fos.close();
    }


    Properties overrides = new Properties();
    overrides.setProperty(FrsProperty.COMPACTOR_LSNGAP_MIN_LOAD.shortName(), "2.00");

    Configuration configuration = Configuration.getConfiguration(directory, overrides);
    
    assertThat(configuration.getString(FrsProperty.COMPACTOR_POLICY), is("bogus123"));
    assertThat(configuration.getDouble(FrsProperty.COMPACTOR_LSNGAP_MIN_LOAD), is(2.00));

    System.setProperty("com.tc.frs." + FrsProperty.COMPACTOR_LSNGAP_MIN_LOAD.shortName(), "1.00");
    
    configuration = Configuration.getConfiguration(directory);

    assertThat(configuration.getString(FrsProperty.COMPACTOR_POLICY), is("bogus123"));
    assertThat(configuration.getDouble(FrsProperty.COMPACTOR_LSNGAP_MIN_LOAD), is(1.00));
  }
}
