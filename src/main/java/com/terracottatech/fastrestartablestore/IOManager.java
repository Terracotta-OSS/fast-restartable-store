/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore;

import java.io.DataInput;
import java.util.Iterator;
import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
public interface IOManager {
  
  Future<Void> append(Chunk chunk);

  <T extends Chunk> Iterator<T> reader(Factory<T> as);
  
  interface Chunk {
    
    long getLowestLsn();
  }
  
  interface Factory<T extends Chunk> {
    
    T construct(DataInput chunk);
  }
}
