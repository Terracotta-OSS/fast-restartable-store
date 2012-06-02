/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import java.io.IOException;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import org.hamcrest.Matchers;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.fail;
import static org.hamcrest.number.OrderingComparison.lessThan;

/**
 * @author tim
 */
public class AtomicCommitListTest {
  private CommitList commitList;

  @Before
  public void setUp() throws Exception {
    commitList = new AtomicCommitList(10, 10, 2000);
  }

  @Test
  public void testBasicAppend() throws Exception {
    LogRecord record0 = record(10);
    assertThat(commitList.append(record0,false), is(true));
    // Test re-append
    assertThat(commitList.append(record0,false), is(false));

    // Test outside of range
    assertThat(commitList.append(record(21),false), is(false));

    commitList.close(10);
    for (LogRecord record : commitList) {
      assertThat(record, is(record0));
    }
  }
  
  @Test
  public void testOneElementSync() throws Exception {
      final long time = System.currentTimeMillis();
      new Thread() {
            @Override
          public void run() {
            try {
                Thread.sleep(1);
            } catch ( InterruptedException ie ) {
                
            }
            commitList.append(record(10), true);
          }
      }.start();
    commitList.waitForContiguous();
    commitList.written();
    commitList.get();
    assertThat(System.currentTimeMillis()-time,lessThan(2000l));
  }

  @Test
  public void testBasicClose() throws Exception {
    LogRecord record0 = record(10);
    assertThat(commitList.append(record0,false), is(true));
    LogRecord record1 = record(11);
    assertThat(commitList.append(record1,false), is(true));

    assertThat(commitList.close(10), is(true));
    assertThat(commitList.isSyncRequested(), is(false));
    for (LogRecord record : commitList) {
      assertThat(record, is(record0));
    }
    
    LogRecord record2 = record(12);
    assertThat(commitList.append(record2,true), is(false));
    assertThat(commitList.isSyncRequested(), is(false));
   
    commitList.next().close(11);
    for (LogRecord record : commitList.next()) {
      assertThat(record, is(record1));
    }
  }

  @Test
  public void testWaitForContiguous() throws Exception {
    assertThat(commitList.append(record(15),false), is(true));
    assertThat(commitList.close(15), is(true));

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
      assertThat(commitList.append(record(i),false), is(false));
      assertThat(commitList.next().append(record(i),false), is(true));
    }

    waiter.join(5 * 1000); // Should still not be done waiting.
    assertThat(waitComplete.get(), is(false));

    for (int i = 10; i < 15; i++) {
      assertThat(commitList.append(record(i),false), is(true));
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
    assertThat(errors,Matchers.<Throwable>empty());
  }
  
  @Test
  public void testThrowingException() throws Exception {
      new Thread() {
          public void run() {
              commitList.exceptionThrown(new IOException());
          }
      }.start();
      
      try {
          commitList.get();
          fail();
      } catch ( ExecutionException ex ) {
          System.out.println("caught exception");
      } catch ( InterruptedException ie ) {
          
      }
  }
  
  
  
  private LogRecord record(long lsn) {
    LogRecord record = mock(LogRecord.class);
    doReturn(lsn).when(record).getLsn();
    return record;
  }

  private void append(LogRecord record) {
    CommitList l = commitList;
    while (!l.append(record,false)) {
      l = l.next();
    }
  }

  private void close(long lsn) {
    CommitList l = commitList;
    while (!l.close(lsn)) {
      l = l.next();
    }
  }
}
