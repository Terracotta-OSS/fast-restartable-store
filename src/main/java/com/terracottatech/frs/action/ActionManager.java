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

  Future<Void> happened(Action action);
  
  //Optimization
  void asyncHappened(Action action);
  
  Action extract(LogRecord record);
}
