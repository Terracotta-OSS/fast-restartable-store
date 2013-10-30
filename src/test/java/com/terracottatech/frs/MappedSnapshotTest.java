/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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
