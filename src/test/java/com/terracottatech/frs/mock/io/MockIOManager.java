/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.io;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.log.LogRegion;
import com.terracottatech.frs.log.LogRegionFactory;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.WrappingChunk;
import com.terracottatech.frs.mock.MockFuture;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
public class MockIOManager implements IOManager {

    private final Deque<byte[]> storage = new LinkedList<byte[]>();

    public MockIOManager() {
    }

    @Override
    public long write(Chunk region) throws IOException {
        byte[] ref = serialize(region);
        storage.push(ref);
        return ref.length;
    }

    @Override
    public void sync() throws IOException {
        //  NOOP
    }

    @Override
    public void setLowestLsn(long lsn) throws IOException {
        //  NOOP
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Future<Void> append(LogRegion logRegion) {
        try {
            storage.push(serialize(logRegion));
            System.out.println(logRegion);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return new MockFuture();
    }

    private byte[] serialize(Chunk c) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        WritableByteChannel chan = Channels.newChannel(out);
        for (ByteBuffer buf : c.getBuffers()) {
            chan.write(buf);
        }
        chan.close();
        return out.toByteArray();
    }

    private byte[] serialize(LogRegion logRegion) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oout = new ObjectOutputStream(bout);
        try {
            oout.writeObject(logRegion);
        } finally {
            oout.close();
        }
        return bout.toByteArray();
    }

    @Override
    public Iterable<Chunk> read(Direction dir) throws IOException {
        ArrayList<Chunk> list = new ArrayList<Chunk>();
        Iterator<byte[]> store =( dir == Direction.FORWARD ) ?
            storage.iterator() : storage.descendingIterator();

        while ( store.hasNext() ) {
            list.add(new WrappingChunk(ByteBuffer.wrap(store.next())));
        }
        
        return list;
    }

    @Override
    public long seek(long lsn) throws IOException {
        return lsn;
    }

    public <T> Iterator<T> reader(final LogRegionFactory<T> as) {
        if (storage.isEmpty()) {
            return Collections.EMPTY_LIST.iterator();
        }
        return new Iterator<T>() {

            private final Iterator<byte[]> delegate = storage.iterator();
            private Iterator<T> current;

            {
                try {
                    current = as.unpack(new WrappingChunk(ByteBuffer.wrap(delegate.next()))).iterator();
                } catch (IOException ioe) {
                    throw new AssertionError(ioe);
                }
            }

            public boolean hasNext() {
                if (current.hasNext()) {
                    return true;
                }
                return switchCurrent();
            }

            private boolean switchCurrent() {
                if (!delegate.hasNext()) {
                    return false;
                }
                try {
                    current = as.unpack(new WrappingChunk(ByteBuffer.wrap(delegate.next()))).iterator();
                } catch (IOException ioe) {
                    throw new AssertionError(ioe);
                }
                return current.hasNext();
            }

            public T next() {
                if (!current.hasNext() && !switchCurrent()) {
                    throw new IndexOutOfBoundsException();
                }
                return current.next();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
