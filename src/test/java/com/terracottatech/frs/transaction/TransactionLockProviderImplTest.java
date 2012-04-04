/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;

/**
 * @author tim
 */
public class TransactionLockProviderImplTest {
  private TransactionLockProvider transactionLockProvider;
  private ExecutorService executorService;


  @Before
  public void setUp() throws Exception {
    transactionLockProvider = new TransactionLockProviderImpl();
    executorService = Executors.newFixedThreadPool(1);
  }

  private Object objectWithHash(final int i) {
    return new Object() {
      @Override
      public int hashCode() {
        return i;
      }
    };
  }

  @Test
  public void testIdLockExcludesKeyLock() throws Exception {
    Object o = new Object();
    Object o1 = new Object();
    final ReadWriteLock idLock = transactionLockProvider.getLockForId(o);
    final ReadWriteLock keyLock = transactionLockProvider.getLockForKey(o, o1);

    idLock.writeLock().lock();
    Future<Boolean> future = executorService.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return keyLock.readLock().tryLock();
      }
    });
    assertThat(future.get(), is(false));

    idLock.writeLock().unlock();
    future = executorService.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return keyLock.readLock().tryLock();
      }
    });
    assertThat(future.get(), is(true));
    assertThat(idLock.readLock().tryLock(), is(true));

    idLock.readLock().unlock();
    assertThat(idLock.writeLock().tryLock(), is(false));
  }

  @Test
  public void testKeyLocksShareId() throws Exception {
    Object id = new Object();
    Object key1 = objectWithHash(1);
    Object key2 = objectWithHash(2);

    final ReadWriteLock keyLock1 = transactionLockProvider.getLockForKey(id, key1);
    final ReadWriteLock keyLock2 = transactionLockProvider.getLockForKey(id, key2);

    keyLock1.writeLock().lock();
    Future<Boolean> future = executorService.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return keyLock2.writeLock().tryLock();
      }
    });
    assertThat(future.get(), is(true));
  }

  @Test
  public void testKeyLocksExclude() throws Exception {
    Object id = new Object();
    Object key1 = objectWithHash(1);
    Object key2 = objectWithHash(1);
    final ReadWriteLock keyLock1 = transactionLockProvider.getLockForKey(id, key1);
    final ReadWriteLock keyLock2 = transactionLockProvider.getLockForKey(id, key2);

    keyLock1.writeLock().lock();
    Future<Boolean> future = executorService.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return keyLock2.writeLock().tryLock();
      }
    });
    assertThat(future.get(), is(false));

    keyLock1.writeLock().unlock();
    keyLock1.readLock().lock();
    future = executorService.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return keyLock2.writeLock().tryLock();
      }
    });
    assertThat(future.get(), is(false));
  }

  @Test
  public void testTimedTryLock() throws Exception {
    Object id = new Object();
    Object key1 = objectWithHash(1);
    Object key2 = objectWithHash(1);
    final ReadWriteLock keyLock1 = transactionLockProvider.getLockForKey(id, key1);
    final ReadWriteLock keyLock2 = transactionLockProvider.getLockForKey(id, key2);

    keyLock1.readLock().lock();
    Future<Boolean> future = executorService.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return keyLock2.writeLock().tryLock(1000, TimeUnit.MILLISECONDS);
      }
    });
    assertThat(future.get(), is(false));

    Future<Long> future1 = executorService.submit(new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        long start = System.nanoTime();
        keyLock2.writeLock().tryLock(5000, TimeUnit.MILLISECONDS);
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      }
    });
    keyLock1.readLock().unlock();
    assertThat(future1.get(), lessThan(5000L));

    executorService.submit(new Runnable() {
      @Override
      public void run() {
        keyLock2.writeLock().unlock();
      }
    }).get();

    final ReadWriteLock idLock = transactionLockProvider.getLockForId(id);
    idLock.writeLock().lock();
    future1 = executorService.submit(new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        long start = System.nanoTime();
        assertThat(keyLock1.readLock().tryLock(5000, TimeUnit.MILLISECONDS), Matchers.is(false));
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      }
    });
    assertThat(future1.get(), lessThan(6000L));
  }
}
