/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

import com.terracottatech.frs.action.Action;

import java.util.Set;

/**
 * @author tim
 */
public class RecoveryActionFactory {
  public Action eviction(Set<Long> invalidatedLsns) {
    return new RecoveryEvictionAction(invalidatedLsns);
  }
}
