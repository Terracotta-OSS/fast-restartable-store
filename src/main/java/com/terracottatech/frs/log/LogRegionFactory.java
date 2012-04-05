/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author cdennis
 */
public interface LogRegionFactory<T> {

  T construct(InputStream chunk) throws IOException;
  
}
