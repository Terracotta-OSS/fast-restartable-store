/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * @author tim
 */
public class AtomicCommitListTest {
  private CommitList commitList;

  @Before
  public void setUp() throws Exception {
    commitList = new AtomicCommitList(true, 10, 10);
  }

  @Test
  public void testBasicAppend() throws Exception {
    LogRecord record0 = record(10);
    assertThat(commitList.append(record0), is(true));
    // Test re-append
    assertThat(commitList.append(record0), is(false));

    // Test outside of range
    assertThat(commitList.append(record(21)), is(false));

    commitList.close(10, false);
    for (LogRecord record : commitList) {
      assertThat(record, is(record0));
    }
  }

  @Test
  public void testBasicClose() throws Exception {
    LogRecord record0 = record(10);
    assertThat(commitList.append(record0), is(true));
    LogRecord record1 = record(11);
    assertThat(commitList.append(record1), is(true));

    assertThat(commitList.close(10, false), is(true));
    assertThat(commitList.isSyncRequested(), is(false));
    for (LogRecord record : commitList) {
      assertThat(record, is(record0));
    }

    assertThat(commitList.next().close(11, true), is(true));
    assertThat(commitList.next().isSyncRequested(), is(true));

    for (LogRecord record : commitList.next()) {
      assertThat(record, is(record1));
    }
  }

  @Test
  public void testWaitForContiguous() throws Exception {
    assertThat(commitList.append(record(15)), is(true));
    assertThat(commitList.close(15, false), is(true));

    final AtomicReference<Exception> error = new AtomicReference<Exception>();
    final AtomicBoolean waitComplete = new AtomicBoolean(false);
    Thread waiter = new Thread() {
      @Override
      public void run() {
        try {
          commitList.waitForContiguous();
          waitComplete.set(true);
        } catch (Exception e) {
          error.set(e);
        }
      }
    };
    waiter.start();
    waiter.join(5 * 1000); // wait for the waiter to start waiting.
    assertThat(waitComplete.get(), is(false));

    for (int i = 16; i < 21; i++) {
      // Try appending a few records that land in the next link
      assertThat(commitList.append(record(i)), is(false));
      assertThat(commitList.next().append(record(i)), is(true));
    }

    waiter.join(5 * 1000); // Should still not be done waiting.
    assertThat(waitComplete.get(), is(false));

    for (int i = 10; i < 15; i++) {
      assertThat(commitList.append(record(i)), is(true));
    }

    waiter.join(5 * 1000);
    assertThat(waitComplete.get(), is(true));
  }

  @Test
  public void testMultiThreadedAppendAndClose() throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(20);
    Random r = new Random();
    final AtomicLong lsn = new AtomicLong(10);
    final List<Throwable> errors =
            Collections.synchronizedList(new ArrayList<Throwable>());
    for (int i = 0; i < 1000; i++) {
      if (r.nextInt(100) < 25) {
        executorService.submit(new Runnable() {
          @Override
          public void run() {
            try {
              append(record(lsn.incrementAndGet()));
            } catch (Throwable t) {
              errors.add(t);
            }
          }
        });
      } else {
        executorService.submit(new Runnable() {
          @Override
          public void run() {
            try {
              close(lsn.get());
            } catch (Throwable t) {
              errors.add(t);
            }
          }
        });
      }
    }
    assertThat((Collection) errors, empty());
  }

  private LogRecord record(long lsn) {
    LogRecord record = mock(LogRecord.class);
    doReturn(lsn).when(record).getLsn();
    return record;
  }

  private void append(LogRecord record) {
    CommitList l = commitList;
    while (!l.append(record)) {
      l = l.next();
    }
  }

  private void close(long lsn) {
    CommitList l = commitList;
    while (!l.close(lsn, false)) {
      l = l.next();
    }
  }
}
