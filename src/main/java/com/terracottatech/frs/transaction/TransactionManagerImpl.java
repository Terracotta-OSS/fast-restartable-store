/*
 * Copyright IBM Corp. 2024, 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author tim
 */
public class TransactionManagerImpl implements TransactionManager {
  private final AtomicLong                               currentTransactionId =
          new AtomicLong();
  private final Map<TransactionHandle, TransactionAccount> liveTransactions     =
          new ConcurrentHashMap<TransactionHandle, TransactionAccount>();

  private final ActionManager           actionManager;

  public TransactionManagerImpl(ActionManager actionManager) {
    this.actionManager = actionManager;
  }

  @Override
  public TransactionHandle begin() {
    TransactionHandle handle =
            new TransactionHandleImpl(currentTransactionId.incrementAndGet());
    TransactionAccount account = new TransactionAccount();
    liveTransactions.put(handle, account);
    return handle;
  }

  @Override
  public void commit(TransactionHandle handle, boolean synchronous) throws TransactionException {
    TransactionAccount account = liveTransactions.remove(handle);
    if (account == null) {
      throw new IllegalArgumentException(
              handle + " does not belong to a live transaction.");
    }
    TransactionCommitAction action = new TransactionCommitAction(handle, account.begin());
    if (synchronous) {
      Future<Void> written = actionManager.syncHappened(action);
      boolean interrupted = false;
      while (true) {
        try {
          written.get();
          break;
        } catch (ExecutionException e) {
          throw new TransactionException("Commit failed.", e);
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    } else {
      actionManager.happened(action);
    }
  }

  @Override
  public void happened(TransactionHandle handle, Action action) {
    TransactionAccount account = liveTransactions.get(handle);
    if (account == null) {
      throw new IllegalArgumentException(
              handle + " does not belong to a live transaction.");
    }
    Action transactionalAction = new TransactionalAction(handle, account.begin(), false, action, account);
    actionManager.happened(transactionalAction);
  }

  @Override
  public long getLowestOpenTransactionLsn() {
    Long lowest = Long.MAX_VALUE;
    for (TransactionAccount account : liveTransactions.values()) {
      lowest = Math.min(account.lsn, lowest);
    }
    return lowest;
  }

  private static class TransactionAccount implements TransactionLSNCallback {
    private long lsn = Long.MAX_VALUE;
    private boolean beginWritten = false;

    synchronized boolean begin() {
      if (beginWritten) {
        return false;
      } else {
        beginWritten = true;
        return true;
      }
    }

    public synchronized void setLsn(long lsn) {
      if (this.lsn == Long.MAX_VALUE) {
        this.lsn = lsn;
      } else {
        // This shouldn't happen as we're getting LSNs in increasing order
        assert lsn > this.lsn;
      }
    }
  }
}
