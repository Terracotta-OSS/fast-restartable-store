/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.config.FrsProperty;
import java.util.Properties;

/**
 *
 * @author mscott
 */
public class MappedBackupTest extends BackupTest {

  @Override
  public Properties configure(Properties props) {
    props.setProperty(FrsProperty.IO_NIO_ACCESS_METHOD.shortName(), "MAPPED");
    return props;
  }
  
}
