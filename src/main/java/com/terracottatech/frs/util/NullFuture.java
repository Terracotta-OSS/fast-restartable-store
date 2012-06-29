/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author tim
 */
public class NullFuture implements Future<Void> {
  public static NullFuture INSTANCE = new NullFuture();

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public Void get() throws InterruptedException, ExecutionException {
    return null;
  }

  @Override
  public Void get(long timeout, TimeUnit unit) throws InterruptedException,
          ExecutionException, TimeoutException {
    return null;
  }
}
