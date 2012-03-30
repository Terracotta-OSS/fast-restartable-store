/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

/**
 * @author tim
 */
public class RestartStoreException extends Exception {
  public RestartStoreException(String message) {
    super(message);
  }

  public RestartStoreException(String message, Throwable cause) {
    super(message, cause);
  }

  public RestartStoreException(Throwable cause) {
    super(cause);
  }

  public RestartStoreException() {

  }
}
