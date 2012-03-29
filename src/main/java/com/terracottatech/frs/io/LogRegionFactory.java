/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author cdennis
 */
public interface LogRegionFactory<T> {

  T construct(InputStream chunk) throws IOException;
  
}
