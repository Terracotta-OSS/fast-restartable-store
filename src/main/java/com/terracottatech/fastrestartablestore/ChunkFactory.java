/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore;

import java.io.DataInput;

/**
 *
 * @author cdennis
 */
public interface ChunkFactory<T extends IOManager.Chunk> {

  T construct(DataInput chunk);
  
}
