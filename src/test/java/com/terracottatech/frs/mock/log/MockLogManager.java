/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.mock.log;

import com.terracottatech.frs.io.LogRegionFactory;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.LogRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author cdennis
 */
public class MockLogManager implements LogManager {
  private final IOManager ioManager;
  private final AtomicLong currentLsn = new AtomicLong();
  
  public MockLogManager(IOManager ioManager) {
    this.ioManager = ioManager;
  }

  public synchronized Future<Void> append(LogRecord record) {
    record.updateLsn(currentLsn.getAndIncrement());
    return ioManager.append(new MockLogRegion(record));
  }

  public Iterator<LogRecord> reader() {
    return ioManager.reader(new MockLogRegionFactory());
  }
  
}

class MockLogRegionFactory implements LogRegionFactory<LogRecord> {
 
  public LogRecord construct(InputStream chunk) throws IOException {
    ObjectInput in = new ObjectInputStream(chunk);
    try {
      return (LogRecord) in.readObject();
    } catch (ClassNotFoundException ex) {
      throw new IOException(ex);
    }
  }
}

