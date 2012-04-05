/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.log;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.log.LogRegionFactory;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.mock.MockFuture;
import com.terracottatech.frs.mock.action.MockLogRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
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
    try {
        ioManager.write(new MockLogRegion(record));
    } catch ( IOException ioe ) {
        ioe.printStackTrace();
    }
    return new MockFuture();
  }

    @Override
    public Future<Void> appendAndSync(LogRecord record) {
        record.updateLsn(currentLsn.getAndIncrement());
        try {
            ioManager.write(new MockLogRegion(record));
        } catch ( IOException ioe ) {
            ioe.printStackTrace();
        }
        return new MockFuture();
    }
  
  

  public Iterator<LogRecord> reader() {
    return ioManager.reader(new MockLogRegionFactory());
  }
  
}
class MockLogRegionFactory implements LogRegionFactory<LogRecord> {
 
  public LogRecord construct(InputStream chunk) throws IOException {
    ObjectInput in = new ObjectInputStream(chunk);
    try {
        return (MockLogRecord)in.readObject();
    } catch (ClassNotFoundException ex) {
      throw new IOException(ex);
    }
  }
}

