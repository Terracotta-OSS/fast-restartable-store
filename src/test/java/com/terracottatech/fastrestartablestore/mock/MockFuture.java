/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author cdennis
 */
class MockFuture implements Future<Void> {

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
