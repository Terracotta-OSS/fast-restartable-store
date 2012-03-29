/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author cdennis
 */
public class MockFuture implements Future<Void> {

  public boolean cancel(boolean bln) {
    return false;
  }

  public boolean isCancelled() {
    return false;
  }

  public boolean isDone() {
    return true;
  }

  public Void get() throws InterruptedException, ExecutionException {
    return null;
  }

  public Void get(long l, TimeUnit tu) throws InterruptedException, ExecutionException, TimeoutException {
    return null;
  }
}
