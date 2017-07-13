/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

public class NotPausedException extends RestartStoreException {
  public NotPausedException() {
  }

  public NotPausedException(String message) {
    super(message);
  }

  public NotPausedException(String message, Throwable cause) {
    super(message, cause);
  }

  public NotPausedException(Throwable cause) {
    super(cause);
  }
}