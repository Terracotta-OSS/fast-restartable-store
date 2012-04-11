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

  long getLsn();

  long getPreviousLsn();

  long getLowestLsn();
  
  void updateLsn(long lsn);
  
  void setLowestLsn(long lsn);
 
  ByteBuffer[] getPayload();
}
