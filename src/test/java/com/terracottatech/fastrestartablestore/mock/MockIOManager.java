/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.Chunk;
import com.terracottatech.fastrestartablestore.ChunkFactory;
import com.terracottatech.fastrestartablestore.IOManager;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author cdennis
 */
class MockIOManager implements IOManager {

  private final Deque<byte[]> storage = new LinkedList<byte[]>();
  
  public MockIOManager() {
  }

  public Future<Void> append(Chunk chunk) {
    try {
      storage.push(serialize(chunk));
      System.out.println(chunk);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
    return new MockFuture();
  }

  private byte[] serialize(Chunk chunk) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ObjectOutputStream oout = new ObjectOutputStream(bout);
    try {
      oout.writeObject(chunk);
    } finally {
      oout.close();
    }
    return bout.toByteArray();
  }
  
  public <T> Iterator<T> reader(final ChunkFactory<T> as) {
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
