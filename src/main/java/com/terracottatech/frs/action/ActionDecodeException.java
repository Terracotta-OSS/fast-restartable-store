/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

import com.terracottatech.frs.RestartStoreException;

/**
 * @author tim
 */
public class ActionDecodeException extends RestartStoreException {
  public ActionDecodeException(String message) {
    super(message);
  }

  public ActionDecodeException(String message, Throwable cause) {
    super(message, cause);
  }

  public ActionDecodeException(Throwable cause) {
    super(cause);
  }

  public ActionDecodeException() {
    super();
  }
}
