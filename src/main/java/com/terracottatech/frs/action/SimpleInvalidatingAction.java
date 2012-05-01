/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

import com.terracottatech.frs.transaction.TransactionLockProvider;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;

/**
 * @author tim
 */
public class SimpleInvalidatingAction implements InvalidatingAction {
  private final Set<Long> invalidatedLsns = new HashSet<Long>();

  public SimpleInvalidatingAction(Set<Long> invalidatedLsns) {
    this.invalidatedLsns.addAll(invalidatedLsns);
  }

  @Override
  public Set<Long> getInvalidatedLsns() {
    return invalidatedLsns;
  }

  @Override
  public void record(long lsn) {
  }

  @Override
  public Set<Long> replay(long lsn) {
    return Collections.emptySet();
  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider lockProvider) {
    return Collections.emptySet();
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    return new ByteBuffer[0];
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SimpleInvalidatingAction that = (SimpleInvalidatingAction) o;

    return invalidatedLsns.equals(that.invalidatedLsns);
  }

  @Override
  public int hashCode() {
    return invalidatedLsns != null ? invalidatedLsns.hashCode() : 0;
  }
}
