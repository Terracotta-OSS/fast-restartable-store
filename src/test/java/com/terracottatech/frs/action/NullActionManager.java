/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

import com.terracottatech.frs.log.LogRecord;

import java.util.concurrent.Future;

/**
 * @author tim
 */
public class NullActionManager implements ActionManager {
  @Override
  public Future<Void> syncHappened(Action action) {
    return null;
  }

  @Override
  public Future<Void> happened(Action action) {
    return null;
  }

  @Override
  public Action extract(LogRecord record) {
    return null;
  }

  @Override
  public void pause() {
  }

  @Override
  public void resume() {
  }
}
