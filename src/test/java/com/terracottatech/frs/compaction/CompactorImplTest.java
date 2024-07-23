/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
package com.terracottatech.frs.compaction;

import org.junit.Before;
import org.junit.Test;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.action.NullActionManager;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.object.NullObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.frs.object.SimpleObjectManagerEntry;
import com.terracottatech.frs.transaction.TransactionManager;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import static com.terracottatech.frs.util.TestUtils.byteBufferWithInt;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author tim
 */
public class CompactorImplTest {
  private CompactionTestObjectManager objectManager;
  private TransactionManager transactionManager;
  private ActionManager actionManager;
  private LogManager logManager;
  private Compactor compactor;
  private TestCompactionPolicy policy;
  private Future<Void> future;

  @Before
  public void setUp() throws Exception {
    future = mock(Future.class);
    objectManager = spy(new CompactionTestObjectManager());
    transactionManager = mock(TransactionManager.class);
    actionManager = spy(new CompactionTestActionManager());
    policy = spy(new TestCompactionPolicy());
    logManager = mock(LogManager.class);
    compactor = new CompactorImpl(objectManager, transactionManager, actionManager,
                                  logManager, policy,
                                  60, 60, 1000, 2000);
  }

  @Test
  public void testBelowCompactionThreshold() throws Exception {
    compactor.startup();
    for (int i = 0; i < 1999; i++) {
      compactor.generatedGarbage(0);
    }
    SECONDS.sleep(1);
    verify(objectManager, never()).getLowestLsn();

    compactor.generatedGarbage(0);

    SECONDS.sleep(1);

    verify(policy).startCompacting();
    verify(policy, never()).stoppedCompacting();
    verify(logManager).updateLowestLsn(anyLong());

    compactor.shutdown();

    verify(actionManager, times(1)).happened(any(Action.class));
    verify(actionManager, times(1)).syncHappened(any(Action.class));
  }

  @Test
  public void testCompactionThresholdTripped() throws Exception {
    policy.compactCount = 1500;
    compactor.startup();

    doReturn(0L).when(logManager).lowestLsn();
    doReturn(1101L).when(logManager).currentLsn();
    doReturn(1100L).when(objectManager).size();

    compactor.compactNow();

    SECONDS.sleep(1);

    policy.waitForCompactionComplete();

    verifyCompactedTimes(1100);
    verify(policy).stoppedCompacting();
    verify(future, atLeastOnce()).get();
    verify(logManager, times(2)).updateLowestLsn(anyLong());
    compactor.shutdown();
  }
  
  @Test
  public void testMaxPermits() throws Exception {
    compactor = new CompactorImpl(objectManager, transactionManager, actionManager,
                                  logManager, policy,
                                  60, 60, 1000, Integer.MAX_VALUE);
//don't start the thread
    compactor.compactNow();
// this should overflow and not cause crashing error
    compactor.generatedGarbage(100);
  }

  @Test
  public void testCompactionTerminatesOnEmptyObjectManager() throws Exception {
    policy.compactCount = 1000;

    compactor.startup();

    doReturn(0L).when(logManager).lowestLsn();
    doReturn(1001L).when(logManager).currentLsn();
    doReturn(1000L).when(transactionManager).getLowestOpenTransactionLsn();
    doReturn(1000L).when(objectManager).size();
    doReturn(null).when(objectManager).acquireCompactionEntry(anyLong());

    compactor.compactNow();

    SECONDS.sleep(1);

    policy.waitForCompactionComplete();

    verifyCompactedTimes(0);
    verify(policy).stoppedCompacting();
    verify(logManager).updateLowestLsn(anyLong());
    compactor.shutdown();
  }

  @Test
  public void testPausing() throws Exception {
    policy.compactCount = 1000;

    doReturn(0L).when(logManager).lowestLsn();
    doReturn(101L).when(logManager).currentLsn();
    doReturn(100L).when(objectManager).size();

    compactor.startup();

    compactor.pause();

    compactor.compactNow();

    verifyCompactedTimes(0);

    compactor.unpause();

    compactor.compactNow();

    SECONDS.sleep(1);

    policy.waitForCompactionComplete();

    verifyCompactedTimes(100);
    verify(policy).stoppedCompacting();
    verify(logManager).updateLowestLsn(anyLong());
    compactor.shutdown();
  }

  private void verifyCompactedTimes(int times) {
    verify(actionManager, times(times)).happened(isA(CompactionAction.class));
    verify(policy, times(times)).compacted(any(ObjectManagerEntry.class));
  }

  private class CompactionTestActionManager extends NullActionManager {
    @Override
    public Future<Void> happened(Action action) {
      action.record(1);
      return future;
    }
     @Override
    public Future<Void> syncHappened(Action action) {
      action.record(1);
      return future;
    }   
  }

  private class CompactionTestObjectManager extends
          NullObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> {
    private ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> compactingEntry;
    private long lsn = 100L;

    @Override
    public ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> acquireCompactionEntry(long ceilingLsn) {
      assert compactingEntry == null;
      lsn++;
      compactingEntry = new SimpleObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer>(
              byteBufferWithInt(1), byteBufferWithInt(2), byteBufferWithInt(3), lsn);
      return compactingEntry;
    }

    @Override
    public void releaseCompactionEntry(ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry) {
      assert compactingEntry != null;
      assert compactingEntry == entry;
      compactingEntry = null;
    }

    @Override
    public void updateLsn(ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry, long newLsn) {
      assert entry == compactingEntry;
    }
  }

  private class TestCompactionPolicy extends NoCompactionPolicy {
    private int compactCount = 0;
    private boolean isCompacting = false;

    @Override
    public boolean startCompacting() {
      assert !isCompacting;
      if (compactCount > 0) {
        isCompacting = true;
        return true;
      } else {
        return false;
      }
    }

    @Override
    public boolean compacted(ObjectManagerEntry<?, ?, ?> entry) {
      assert isCompacting;
      return --compactCount > 0;
    }

    @Override
    public synchronized void stoppedCompacting() {
      assert isCompacting;
      isCompacting = false;
      notifyAll();
    }

    synchronized void waitForCompactionComplete() throws InterruptedException {
      while(isCompacting) {
        wait();
      }
    }
  }
}
