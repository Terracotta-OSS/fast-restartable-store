/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

import com.terracottatech.frs.MapActionFactory;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionActionFactory;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * @author tim
 */
public class RecoveryManagerImplTest {
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
    mapActionFactory = new MapActionFactory(objectManager);
    logManager = newLogManager();
    actionManager = newActionManager();
    recoveryManager = new RecoveryManagerImpl(logManager, actionManager);
  }

  private LogManager newLogManager() {
    LogManager lm = new TestLogManager();
    return spy(lm);
  }

  private ActionManager newActionManager() {
    return mock(ActionManager.class);
  }

  private LogRecord record(long lsn, long previousLsn, Action action) throws Exception {
    LogRecord record = mock(LogRecord.class);
    doReturn(lsn).when(record).getLsn();
    doReturn(previousLsn).when(record).getPreviousLsn();
    doReturn(action).when(actionManager).extract(record);
    return record;
  }

  @Test
  public void testRecover() throws Exception {
    Action skippedAction = mock(Action.class);
    doThrow(new AssertionError("Should not have been executed.")).when(skippedAction).replay(anyLong());

    // Try skipping something...
    logManager.append(record(8, -1, skippedAction));
    Action skipper = mock(Action.class);
    logManager.append(record(9, 8, skipper));

    // Check that an action skipped by one in a transaction is properly skipped
    logManager.append(record(10, -1, skippedAction));
    logManager.append(record(11, -1, transactionActionFactory.transactionBegin(1)));
    Action validTransactional = mock(Action.class);
    logManager.append(record(12, 10, transactionActionFactory.transactionalAction(1, validTransactional)));
    logManager.append(record(13, -1, transactionActionFactory.transactionCommit(1)));

    // Test a torn transaction
    logManager.append(record(14, -1, transactionActionFactory.transactionBegin(2)));
    logManager.append(record(15, 12, transactionActionFactory.transactionalAction(2, skippedAction)));

    // Try out a deleted action
    logManager.append(record(16, -1, skipped(mapActionFactory.put(1, 2, 3))));
    logManager.append(record(17, -1, skipped(mapActionFactory.put(1, 3, 5))));
    logManager.append(record(18, -1, skipped(mapActionFactory.put(1, 4, 5))));
    Action checkedPut = spy(mapActionFactory.put(2, 3, 4));
    logManager.append(record(19, -1, checkedPut));
    logManager.append(record(20, -1, mapActionFactory.delete(1)));

    recoveryManager.recover();

    verify(skipper).replay(9);
    verify(validTransactional).replay(12);
    verify(checkedPut).replay(19);
  }

  private Action skipped(Action action) {
    Action a = spy(action);
    doThrow(new AssertionError("Should not have been executed.")).when(a).replay(anyLong());
    return a;
  }

  private class TestLogManager implements LogManager {
    private final List<LogRecord> records = new LinkedList<LogRecord>();

    @Override
    public void startup() {
    }

    @Override
    public void shutdown() {
    }

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
    public Iterator<LogRecord> reader() {
      return records.iterator();
    }
  }
}
