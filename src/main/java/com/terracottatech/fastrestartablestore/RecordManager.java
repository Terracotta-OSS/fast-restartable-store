/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore;

import com.terracottatech.fastrestartablestore.messages.LogRecord;
import com.terracottatech.fastrestartablestore.messages.Action;
import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
public interface RecordManager {

  Future<Void> happened(Action action);
  
  //Optimization
  void asyncHappened(Action action);
  
  Action extract(LogRecord record) throws IllegalArgumentException;

}
