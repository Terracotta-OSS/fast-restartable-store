/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

import java.util.Set;

/**
 * @author tim
 */
public interface InvalidatingAction extends Action {
  Set<Long> getInvalidatedLsns();
}
