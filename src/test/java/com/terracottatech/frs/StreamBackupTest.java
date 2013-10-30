/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.terracottatech.frs;

import com.terracottatech.frs.config.FrsProperty;
import java.util.Properties;

/**
 *
 * @author mscott
 */
public class StreamBackupTest extends BackupTest {
  @Override
  public Properties configure(Properties props) {
    props.setProperty(FrsProperty.IO_NIO_ACCESS_METHOD.shortName(), "STREAM");
    return props;
  }
}
