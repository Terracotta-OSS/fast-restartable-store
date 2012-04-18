/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.log;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.log.LogRegionFactory;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.log.BufferListWrapper;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.mock.MockFuture;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author cdennis
 */
public class MockLogManager implements LogManager {

    private final IOManager ioManager;
    private final AtomicLong currentLsn = new AtomicLong();
    LogRegionFactory packer = new MockLogRegionFactory();

    public MockLogManager(IOManager ioManager) {
        this.ioManager = ioManager;
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
//  lame but assume one chunk for mock 
        try {
            Iterable<Chunk> list = ioManager.read(Direction.REVERSE);
            Iterator<Chunk> chunks = list.iterator();
            if ( !chunks.hasNext() ) return Collections.<LogRecord>emptyList().iterator();
            ArrayList<LogRecord> records = new ArrayList<LogRecord>();
            while ( chunks.hasNext() ) {
                records.addAll(new MockLogRegionFactory().unpack(chunks.next()));
            }
            Collections.reverse(records);
            return records.iterator();
        } catch ( IOException ioe ) {
            throw new AssertionError(ioe);
        }
    }

static class MockLogRegionFactory implements LogRegionFactory<LogRecord> {

    @Override
    public Chunk pack(Iterable<LogRecord> payload) throws IOException {
        ArrayList<ByteBuffer> list = new ArrayList<ByteBuffer>();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(bos);        
        for ( LogRecord record : payload ) {
            oo.writeObject(record);
        }
        oo.close();
        list.add(ByteBuffer.wrap(bos.toByteArray()));
        return new BufferListWrapper(list);
        
    }

    @Override
    public List<LogRecord> unpack(Chunk data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        WritableByteChannel w = Channels.newChannel(bos);
        for ( ByteBuffer buf : data.getBuffers(data.length()) ) {
            w.write(buf);
        }
        w.close();
        ByteArrayInputStream chunk = new ByteArrayInputStream(bos.toByteArray());
        ObjectInput in = new ObjectInputStream(chunk);
        ArrayList<LogRecord> list = new ArrayList<LogRecord>();
        try {
                list.add((LogRecord)in.readObject());
        } catch (ClassNotFoundException ex) {
            throw new IOException(ex);
        } catch ( EOFException eof ) {
            
        }
        return list;
    }
}
}
