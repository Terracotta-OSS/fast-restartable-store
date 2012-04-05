/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import com.terracottatech.frs.log.LogRegionFactory;
import java.io.IOException;
import java.util.Iterator;

/**
 *
 * @author cdennis
 */
public interface IOManager {
  
  long write(Chunk region) throws IOException;
  
  void setLowestLsn(long lsn) throws IOException;

  <T> Iterator<T> reader(LogRegionFactory<T> as);
  
  void sync() throws IOException;
}
