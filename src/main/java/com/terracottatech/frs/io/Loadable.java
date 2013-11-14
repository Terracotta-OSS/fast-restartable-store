/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.frs.io;

import java.io.IOException;

/**
 *
 * @author mscott
 */
public interface Loadable {
  void load() throws IOException;
}
