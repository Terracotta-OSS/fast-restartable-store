/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
public interface RecoveryManager {

  public Future<Void> recover(RecoveryListener ... listeners) throws RecoveryException,
          InterruptedException;
  
}
