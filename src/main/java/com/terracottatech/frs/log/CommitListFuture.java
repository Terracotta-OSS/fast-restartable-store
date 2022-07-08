/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.frs.log;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author mscott
 */
public class CommitListFuture implements Future<Void> {
  
  final private CommitList origin;
  final private long lsn;

  public CommitListFuture(long lsn, CommitList origin) {
    this.origin = origin;
    this.lsn = lsn;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    CommitList target = origin;
    while ( target.getWriteFuture().isDone() ) {
      if ( target.getEndLsn() >= lsn ) {
        return true;
      } else {
        target = target.next();
      }
    }
    return false;
  }

  @Override
  public Void get() throws InterruptedException, ExecutionException {
    CommitList target = origin;
    target.getWriteFuture().get();
    while ( target.getEndLsn() < lsn ) {
      target = target.next();
      target.getWriteFuture().get();
    }
    return null;
  }

  @Override
  public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    CommitList target = origin;
    long to = unit.toNanos(timeout) + System.nanoTime();
    target.getWriteFuture().get(timeout,unit);
    while ( target.getEndLsn() < lsn ) {
      target = target.next();
      long ct = System.nanoTime();
      if ( ct > to ) {
        throw new TimeoutException();
      }
      target.getWriteFuture().get(to - ct, TimeUnit.NANOSECONDS);

    }
    return null;
  }
  
}
