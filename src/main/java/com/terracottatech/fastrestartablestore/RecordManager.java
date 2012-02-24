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
public interface RecordManager<K, V> {

  Future<Void> happened(Action<K, V> action);
  
  //Optimization
  void asyncHappened(Action<K, V> action);
  
  Action<K, V> extract(LogRecord record) throws IllegalArgumentException;
}
