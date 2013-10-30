/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
