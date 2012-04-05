/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public interface LogRecord {

  //private final byte[] data;
  //private final byte[] data;
  long getLsn();

  long getPreviousLsn();

  long getLowestLsn();
  
  void updateLsn(long lsn);
    
  ByteBuffer[] getPayload();
}
