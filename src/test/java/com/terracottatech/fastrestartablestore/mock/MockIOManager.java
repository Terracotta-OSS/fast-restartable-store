/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.Chunk;
import com.terracottatech.fastrestartablestore.ChunkFactory;
import com.terracottatech.fastrestartablestore.IOManager;
import java.util.Iterator;
import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
class MockIOManager implements IOManager {

  public MockIOManager() {
  }

  public Future<Void> append(Chunk chunk) {
    System.out.println(chunk);
    return new MockFuture();
  }

  public <T extends Chunk> Iterator<T> reader(ChunkFactory<T> as) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
}
