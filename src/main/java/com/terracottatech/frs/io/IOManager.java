/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io;

import java.util.Iterator;
import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
public interface IOManager {
  
  Future<Void> append(LogRegion logRegion);

  <T> Iterator<T> reader(LogRegionFactory<T> as);
}
