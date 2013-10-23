/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

import com.terracottatech.frs.DisposableAction;
import com.terracottatech.frs.Disposable;
import com.terracottatech.frs.GettableAction;
import com.terracottatech.frs.GettableDisposableAction;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.log.LogRecordFactory;
import com.terracottatech.frs.object.ObjectManager;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

/**
 * @author tim
 */
public class ActionManagerImpl implements ActionManager {
  private final LogManager             logManager;
  private final ObjectManager<?, ?, ?> objectManager;
  private final ActionCodec            actionCodec;
  private final LogRecordFactory       logRecordFactory;

  public ActionManagerImpl(LogManager logManager, ObjectManager<?, ?, ?> objectManager,
                           ActionCodec actionCodec, LogRecordFactory logRecordFactory) {
    this.logManager = logManager;
    this.objectManager = objectManager;
    this.actionCodec = actionCodec;
    this.logRecordFactory = logRecordFactory;
  }

  private LogRecord wrapAction(Action action) {
    ByteBuffer[] payload = actionCodec.encode(action);
    return logRecordFactory.createLogRecord(payload, action);
  }

  @Override
  public Future<Void> syncHappened(Action action) {
    return logManager.appendAndSync(wrapAction(action));
  }

  @Override
  public Future<Void> happened(Action action) {
    return logManager.append(wrapAction(action));
  }

  @Override
  public Action extract(LogRecord record) {
    Action a = actionCodec.decode(record.getPayload());
    if ( record instanceof Disposable ) {
        return a instanceof GettableAction ?
                new GettableDisposableAction((GettableAction)a, (Disposable)record) :
                new DisposableAction(a, (Disposable)record);
    } else {
        return a;
  }
}
}
