/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.terracottatech.frs;

import com.terracottatech.frs.config.FrsProperty;
import org.junit.Before;

/**
 *
 * @author mscott
 */
public class MappedSnapshotTest extends SnapshotTest {
  
  @Before
  public void setupPropertiesOverride() {
    properties.setProperty(FrsProperty.IO_NIO_ACCESS_METHOD.shortName(), "MAPPED");
  }
}
