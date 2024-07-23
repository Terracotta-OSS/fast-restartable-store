/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs.mock.io;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.IOStatistics;
import com.terracottatech.frs.io.WrappingChunk;
import com.terracottatech.frs.log.FormatException;
import com.terracottatech.frs.log.LogRegion;
import com.terracottatech.frs.log.LogRegionFactory;
import com.terracottatech.frs.mock.MockFuture;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
public class MockIOManager implements IOManager {

    private final LinkedList<byte[]> storage = new LinkedList<byte[]>();
    ListIterator<byte[]> store = storage.listIterator();

    public MockIOManager() {
    }

    @Override
    public long write(Chunk region, long lsn) throws IOException {
        byte[] ref = serialize(region);
        storage.push(ref);
        return ref.length;
    }

    @Override
    public void sync() throws IOException {
        //  NOOP
    }


    @Override
    public void setMinimumMarker(long lsn) throws IOException {
    }

    @Override
    public long getCurrentMarker() throws IOException {
        return 0;
    }

    @Override
    public long getMinimumMarker() throws IOException {
        return 0;
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
    public Chunk read(Direction dir) throws IOException {
        if ( dir == Direction.FORWARD ) {
            if ( !store.hasPrevious() ) return null;
            return new WrappingChunk(ByteBuffer.wrap(store.previous()));
        } else {
            if ( !store.hasNext() ) return null;
            return new WrappingChunk(ByteBuffer.wrap(store.next()));
        }
        
    }

    @Override
    public Future<Void> clean(long timeout) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    @Override
    public long seek(long lsn) throws IOException {
        if ( lsn < 0 ) {
            store = storage.listIterator();
        } else {
            Collections.reverse(storage);
            store = storage.listIterator();
        }
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
                } catch ( FormatException ce ) {
                    throw new RuntimeException(ce);
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
                return current.hasNext();
                                } catch ( FormatException ce ) {
                    throw new RuntimeException(ce);
                }
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

    @Override
    public IOStatistics getStatistics() throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Chunk scan(long marker) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    
    }
