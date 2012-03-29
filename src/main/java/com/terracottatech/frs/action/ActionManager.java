/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
  
  Action extract(LogRecord record) throws IllegalArgumentException;
}
