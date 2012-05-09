/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

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
    long lowestLsn = objectManager.getLowestLsn();
    ByteBuffer[] payload = actionCodec.encode(action);
    return logRecordFactory.createLogRecord(lowestLsn, payload, action);
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
  public void asyncHappened(Action action) {
    logManager.append(wrapAction(action));
  }

  @Override
  public Action extract(LogRecord record) {
    return actionCodec.decode(record.getPayload());
  }
}
