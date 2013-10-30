/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.frs;

import static com.terracottatech.frs.RestartStoreReadTest.properties;
import com.terracottatech.frs.config.FrsProperty;
import org.junit.BeforeClass;

/**
 *
 * @author mscott
 */
public class MappedRestartStoreReadTest extends RestartStoreReadTest {
  
  public MappedRestartStoreReadTest() {
  }
  
  @BeforeClass
  public static void setUpClassOverride() {
    properties.setProperty(FrsProperty.IO_NIO_ACCESS_METHOD.shortName(), "MAPPED");
  }
  
}
