/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.frs.flash;

import com.terracottatech.frs.log.LogRecord;
import java.io.IOException;

/**
 *
 * @author mscott
 */
public interface ReadManager {
  LogRecord get(long marker) throws IOException;
}
