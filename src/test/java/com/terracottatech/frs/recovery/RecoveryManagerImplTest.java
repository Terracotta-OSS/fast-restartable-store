/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

import com.terracottatech.frs.Constants;
import com.terracottatech.frs.ExposedDeleteAction;
import com.terracottatech.frs.MapActionFactory;
import com.terracottatech.frs.PutAction;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.action.InvalidatingAction;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.log.NullLogManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.ExposedTransactionalAction;
import com.terracottatech.frs.transaction.TransactionActionFactory;
import com.terracottatech.frs.transaction.TransactionHandle;
import com.terracottatech.frs.util.JUnitTestFolder;
import com.terracottatech.frs.util.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * @author tim
 */
public class RecoveryManagerImplTest {
  @Rule
  public JUnitTestFolder testFolder = new JUnitTestFolder();

  private ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private TransactionActionFactory transactionActionFactory;
  private MapActionFactory mapActionFactory;
  private LogManager logManager;
  private ActionManager actionManager;
  private RecoveryManager recoveryManager;

  @Before
  public void setUp() throws Exception {
    objectManager = mock(ObjectManager.class);
    transactionActionFactory = new TransactionActionFactory();
    mapActionFactory = new MapActionFactory(objectManager, mock(Compactor.class));
    logManager = newLogManager();
    actionManager = newActionManager();
    recoveryManager = new RecoveryManagerImpl(logManager, actionManager, Configuration.getConfiguration(testFolder.newFolder()));
  }

  private LogManager newLogManager() {
    LogManager lm = new RecoveryTestLogManager();
    return spy(lm);
  }

  private ActionManager newActionManager() {
    return mock(ActionManager.class);
  }

  private LogRecord record(long lsn, Action action) throws Exception {
    LogRecord record = mock(LogRecord.class);
    doReturn(lsn).when(record).getLsn();
    doReturn(action).when(actionManager).extract(record);
    return record;
  }

  private Action action(long previousLsn, boolean shouldReplay) {
    InvalidatingAction action = mock(InvalidatingAction.class);
    doReturn(Collections.singleton(previousLsn)).when(action).getInvalidatedLsns();
    if (!shouldReplay) {
      doThrow(new AssertionError("Should not have been executed.")).when(action).replay(
              anyLong());
    }
    return action;
  }

  private Action action(boolean shouldReplay) {
    Action action = mock(Action.class);
    if (!shouldReplay) {
      doThrow(new AssertionError("Should not have been executed.")).when(action).replay(
              anyLong());
    }
    return action;
  }

  @Test
  public void testRecover() throws Exception {
    // Try skipping something...
    logManager.append(record(8, action(false)));
    Action skipper = action(8, true);
    logManager.append(record(9, skipper));

    // Check that an action skipped by one in a transaction is properly skipped
    logManager.append(record(10, action(false)));
    Action validTransactional = action(10, true);
    logManager.append(record(12, transactionActionFactory.transactionalAction(1, validTransactional, true)));
    logManager.append(record(13, transactionActionFactory.transactionCommit(1)));

    // Test a torn transaction
    logManager.append(record(15, transactionActionFactory.transactionalAction(2, action(12, false), true)));

    // Try out a deleted action
    logManager.append(record(16, skipped(mapActionFactory.put(1, 2, 3))));
    logManager.append(record(17, skipped(mapActionFactory.put(1, 3, 5))));
    logManager.append(record(18, skipped(mapActionFactory.put(1, 4, 5))));
    Action checkedPut = spy(mapActionFactory.put(2, 3, 4));
    logManager.append(record(19, checkedPut));
    logManager.append(record(20, mapActionFactory.delete(1)));

    logManager.updateLowestLsn(8);

    recoveryManager.recover();

    verify(skipper).replay(9);
    verify(validTransactional).replay(12);
    verify(checkedPut).replay(19);
  }
  
    @Test
  public void testRecoverZeroItems() throws Exception {

    recoveryManager.recover();

  }
    
    @Test
  public void testRecoverOneItem() throws Exception {
    logManager.append(record(8, action(true)));
    logManager.updateLowestLsn(8);

    recoveryManager.recover();
  }
    
  @Test
  public void testRecoveryError() throws Exception {
    Action errorAction = mock(Action.class);
    doThrow(new AssertionError()).when(errorAction).replay(anyLong());
    logManager.append(record(Constants.FIRST_LSN, errorAction));

    try {
      recoveryManager.recover();
      fail();
    } catch (RecoveryException e) {
      // Expected
    }
  }

  @Test
  public void testMissingRecordsOnRecovery() throws Exception {
    logManager.append(record(200, action(true)));
    logManager.updateLowestLsn(Constants.FIRST_LSN);

    try {
      recoveryManager.recover();
      fail();
    } catch (RecoveryException e) {
      // Expected
    }
  }
  
  
  @Test
  public void testDisposal() throws Exception {
    TransactionHandle handle = new TransactionHandle() {

      @Override
      public ByteBuffer toByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(1);
        buffer.flip();
        return buffer;
      }
    };
    ExposedDeleteAction delete = mock(ExposedDeleteAction.class);
    when(delete.getId()).thenReturn(TestUtils.byteBufferWithInt(1));
    ExposedDeleteAction wrappedDelete = mock(ExposedDeleteAction.class);
    when(wrappedDelete.getId()).thenReturn(TestUtils.byteBufferWithInt(1));
    PutAction put = mock(PutAction.class);
    PutAction wrappedPut = mock(PutAction.class);
    ExposedTransactionalAction putTransaction = new ExposedTransactionalAction(handle, true, true, wrappedPut, null);
    ExposedTransactionalAction deleteTransaction = new ExposedTransactionalAction(handle, true, true, wrappedDelete, null);
    
    logManager.append(record(200, delete));
    logManager.append(record(201, put));
    logManager.append(record(203, putTransaction));
    logManager.append(record(204, deleteTransaction));
    logManager.updateLowestLsn(Constants.FIRST_LSN);

    try {
      recoveryManager.recover();
      fail();
    } catch (RecoveryException e) {
      // Expected
    }
    verify(put).dispose();
    verify(wrappedPut).dispose();
    verify(delete).dispose();
    verify(wrappedDelete).dispose();
  }

  private Action skipped(Action action) {
    Action a = spy(action);
    doThrow(new AssertionError("Should not have been executed.")).when(a).replay(anyLong());
    return a;
  }

  private class RecoveryTestLogManager extends NullLogManager {
    private final List<LogRecord> records = new LinkedList<LogRecord>();
    private long lowestLsn = Long.MAX_VALUE;

    @Override
    public Future<Void> append(LogRecord record) {
      records.add(0, record);
      return mock(Future.class);
    }

    @Override
    public Future<Void> appendAndSync(LogRecord record) {
      return append(record);
    }

    @Override
    public Iterator<LogRecord> startup() {
      return records.iterator();
    }

    @Override
    public void updateLowestLsn(long lsn) {
      lowestLsn = lsn;
    }

    @Override
    public long lowestLsn() {
      return lowestLsn;
    }
  }
}
