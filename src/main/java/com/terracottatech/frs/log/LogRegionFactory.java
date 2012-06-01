/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.Chunk;
import java.util.List;

/**
 *
 * @author cdennis
 */
public interface LogRegionFactory<T> {

  Chunk pack(Iterable<T> payload);
  List<T> unpack(Chunk data) throws FormatException ;
}
