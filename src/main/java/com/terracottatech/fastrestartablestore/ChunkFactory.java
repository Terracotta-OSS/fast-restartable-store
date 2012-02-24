/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author cdennis
 */
public interface ChunkFactory<T> {

  T construct(InputStream chunk) throws IOException;
  
}
