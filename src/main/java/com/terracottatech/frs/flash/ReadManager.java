/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.frs.flash;

import com.terracottatech.frs.log.LogRecord;

/**
 *
 * @author mscott
 */
public interface ReadManager {
  LogRecord get(long marker);
}
