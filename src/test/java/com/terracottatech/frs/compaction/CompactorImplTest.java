/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.compaction;

import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.object.NullObjectManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.frs.object.SimpleObjectManagerEntry;
import com.terracottatech.frs.transaction.NullTransactionManager;
import com.terracottatech.frs.transaction.TransactionManager;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static com.terracottatech.frs.util.TestUtils.byteBufferWithInt;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.*;

/**
 * @author tim
 */
public class CompactorImplTest {
  private ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private LogManager logManager;
  private TransactionManager transactionManager;
  private Compactor compactor;

  @Before
  public void setUp() throws Exception {
    objectManager = spy(new CompactionTestObjectManager());
    logManager = mock(LogManager.class);
    transactionManager = spy(new CompactionTestTransactionManager());
    compactor = new CompactorImpl(objectManager, transactionManager, logManager);
  }

  @Test
  public void testBelowCompactionThreshold() throws Exception {
    compactor.startup();
    for (int i = 0; i < 999; i++) {
      compactor.generatedGarbage();
    }
    SECONDS.sleep(1);
    verify(objectManager, never()).updateLowestLsn();

    compactor.generatedGarbage();

    SECONDS.sleep(1);

    compactor.shutdown();

    verify(transactionManager, never()).happened(any(CompactionAction.class));
  }

  @Test
  public void testCompactionThresholdTripped() throws Exception {
    compactor.startup();

    doReturn(200L).when(logManager).currentLsn();
    doReturn(100L).when(objectManager).getLowestLsn();
    doReturn(40L).when(objectManager).size();

    compactor.compactNow();

    SECONDS.sleep(1);

    compactor.shutdown();

    verify(transactionManager, atLeastOnce()).happened(any(CompactionAction.class));
  }

  private class CompactionTestTransactionManager extends NullTransactionManager {
    @Override
    public void happened(Action action) throws InterruptedException,
            TransactionException {
      action.record(1);
    }
  }

  private class CompactionTestObjectManager extends
          NullObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> {
    private ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> compactingEntry;
    private long lsn = 100L;

    @Override
    public ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> acquireCompactionEntry(long ceilingLsn) {
      assert compactingEntry == null;
      if (lsn % 3 == 0) {
        return null;
      }
      compactingEntry = new SimpleObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer>(
              byteBufferWithInt(1), byteBufferWithInt(2), byteBufferWithInt(3), lsn++);
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
}
