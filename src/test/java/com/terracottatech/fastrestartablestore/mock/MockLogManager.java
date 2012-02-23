/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.IOManager;
import com.terracottatech.fastrestartablestore.LogManager;
import com.terracottatech.fastrestartablestore.messages.LogRecord;
import com.terracottatech.fastrestartablestore.messages.LogRegion;
import java.util.Iterator;
import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
class MockLogManager implements LogManager {

  private final IOManager ioManager;
  
  public MockLogManager(IOManager ioManager) {
    this.ioManager = ioManager;
  }

  public Future<Void> append(LogRecord record) {
    return ioManager.append(new MockLogRegion(record));
  }

  public Iterator<LogRecord> reader() {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
}
