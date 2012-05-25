/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

import com.terracottatech.frs.log.LogRecord;

import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
public interface ActionManager {

  /**
   * Record the action and initiate a sync.
   *
   * @param action {@link Action} to record into the log stream
   * @return {@link Future} representing the disk write for the action.
   */
  Future<Void> syncHappened(Action action);

  /**
   * Record the given {@link Action} into the log stream.
   *
   * @param action {@link Action} to record
   * @return {@link Future} that represents when the {@link Action} is written to disk.
   */
  Future<Void> happened(Action action);

  /**
   * Extrac the {@link Action} from the given {@link LogRecord}
   *
   * @param record {@link LogRecord} to pull the {@link Action} out of.
   * @return {@link Action}
   */
  Action extract(LogRecord record);
}
