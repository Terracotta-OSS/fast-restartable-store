/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.Closeable;
import java.io.IOException;

/**
 *
 * @author cdennis
 */
public interface IOManager extends Closeable {
  
  long write(Chunk region) throws IOException;
  
  void setLowestLsn(long lsn) throws IOException;
  
  Chunk read(Direction dir) throws IOException;
  
  long seek(long lsn) throws IOException;
  
  void sync() throws IOException;
  
  public enum Seek {
    BEGINNING (0),
    END (-1);
    
    Seek(long value) {
        this.value = value;
    }
    
    private long value;
    
    public long getValue() {
        return value;
    }
}
  
}
