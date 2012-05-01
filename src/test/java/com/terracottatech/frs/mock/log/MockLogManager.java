/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.log;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.log.BufferListWrapper;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.log.LogRegionFactory;
import com.terracottatech.frs.mock.MockFuture;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author cdennis
 */
public class MockLogManager implements LogManager {

    private final IOManager ioManager;
    private final AtomicLong currentLsn = new AtomicLong();
    LogRegionFactory packer = new MockLogManager.MockLogRegionFactory();

    public MockLogManager(IOManager ioManager) {
        this.ioManager = ioManager;
    }

  @Override
  public long currentLsn() {
    return currentLsn.get();
  }

  @Override
  public void startup() {
  }

    @Override
    public void shutdown() {
    }

    public synchronized Future<Void> append(LogRecord record) {
        record.updateLsn(currentLsn.getAndIncrement());
        try {
            ioManager.write(packer.pack(new MockLogRegion(record)));
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return new MockFuture();
    }

    @Override
    public Future<Void> appendAndSync(LogRecord record) {
        record.updateLsn(currentLsn.getAndIncrement());
        try {
            ioManager.write(packer.pack(new MockLogRegion(record)));
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return new MockFuture();
    }

    public Iterator<LogRecord> reader() {
        try {
            ioManager.seek(IOManager.Seek.END.getValue());
            Chunk cc = ioManager.read(Direction.REVERSE);
            if (cc == null) {
                return Collections.<LogRecord>emptyList().iterator();
            }
            ArrayList<LogRecord> records = new ArrayList<LogRecord>();
            while (cc != null) {
                List<LogRecord> rlist = new MockLogRegionFactory().unpack(cc);
                Collections.reverse(rlist);
                records.addAll(rlist);
                cc = ioManager.read(Direction.REVERSE);
            }
            return records.iterator();
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }
    }

    static class MockLogRegionFactory implements LogRegionFactory<LogRecord> {

        @Override
        public Chunk pack(Iterable<LogRecord> payload) {
            try {
                ArrayList<ByteBuffer> list = new ArrayList<ByteBuffer>();
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oo = new ObjectOutputStream(bos);
                for (LogRecord record : payload) {
                    oo.writeObject(record);
                }
                oo.close();
                list.add(ByteBuffer.wrap(bos.toByteArray()));
                return new BufferListWrapper(list);
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }

        }

        @Override
        public List<LogRecord> unpack(Chunk data) {
            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                WritableByteChannel w = Channels.newChannel(bos);
                for (ByteBuffer buf : data.getBuffers(data.length())) {
                    w.write(buf);
                }
                w.close();
                ByteArrayInputStream chunk = new ByteArrayInputStream(bos.toByteArray());
                ObjectInput in = new ObjectInputStream(chunk);
                return Collections.singletonList((LogRecord) in.readObject());
            } catch (ClassNotFoundException ex) {
                throw new RuntimeException(ex);                
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
    }
}
