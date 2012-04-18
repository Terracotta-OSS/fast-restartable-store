/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.IOException;

/**
 *
 * @author cdennis
 */
public interface IOManager {
  
  long write(Chunk region) throws IOException;
  
  void setLowestLsn(long lsn) throws IOException;
  
  Chunk read(Direction dir) throws IOException;
  
  long seek(long lsn) throws IOException;
  
  void sync() throws IOException;
  
  void close() throws IOException;  
}
