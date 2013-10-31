/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import java.io.Closeable;
import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public interface LogRecord extends Closeable {

  long getLsn();
  
  void updateLsn(long lsn);
   
  ByteBuffer[] getPayload();
}
