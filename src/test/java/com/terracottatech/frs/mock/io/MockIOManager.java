/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.io;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.log.LogRegion;
import com.terracottatech.frs.log.LogRegionFactory;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.mock.MockFuture;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
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
      for ( ByteBuffer buf : c.getBuffers() ) {
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
  
  public <T> Iterator<T> reader(final LogRegionFactory<T> as) {
    return new Iterator<T>() {

      private final Iterator<byte[]> delegate = storage.iterator();
      
      public boolean hasNext() {
        return delegate.hasNext();
      }

      public T next() {
        try {
          return as.construct(new ByteArrayInputStream(delegate.next()));
        } catch (IOException ex) {
          //this will likely have to propagate up to the RecoveryManager class - Iterator contract means it must be runtime!
          throw new AssertionError(ex);
        }
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
  
}
