package com.terracottatech.frs.recovery;

import com.terracottatech.frs.RestartStoreException;

/**
 * @author tim
 */
public class RecoveryException extends RestartStoreException {
  public RecoveryException(String msg) {
    super(msg);
  }

  public RecoveryException(String msg, Throwable t) {
    super(msg, t);
  }
}
