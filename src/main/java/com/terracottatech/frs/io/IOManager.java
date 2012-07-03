/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
public interface IOManager extends Closeable {
  
  long write(Chunk region, long marker) throws IOException;
  
  void setMinimumMarker(long marker) throws IOException;
  long getMinimumMarker() throws IOException;
  
//  void setMaximumMarker(long marker) throws IOException;
//  long getMaximumMarker() throws IOException;
  
//  void setCurrentMarker(long marker) throws IOException;
  long getCurrentMarker() throws IOException;
  
  Chunk read(Direction dir) throws IOException;
  
  long seek(long marker) throws IOException;
  
  void sync() throws IOException;
  
  Future<Void> clean(long timeout) throws IOException;
  
  IOStatistics getStatistics() throws IOException;
  
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
