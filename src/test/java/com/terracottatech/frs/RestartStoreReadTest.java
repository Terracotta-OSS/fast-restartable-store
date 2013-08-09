/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.heap.HeapObjectManager;
import java.io.File;
import java.nio.ByteBuffer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.terracottatech.frs.util.TestUtils.byteBufferWithInt;
import java.util.Arrays;
import junit.framework.Assert;

/**
 *
 * @author mscott
 */
public class RestartStoreReadTest  {
  
  @Rule
  public TemporaryFolder folder= new TemporaryFolder();

  RestartStore  restart;
  ObjectManager<ByteBuffer,ByteBuffer,ByteBuffer> omgr;
  
  public RestartStoreReadTest() {
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  @Before
  public void setUp() throws Throwable {
    File temp = folder.newFolder();

    omgr = new HeapObjectManager<ByteBuffer,ByteBuffer,ByteBuffer>(1);
//    TransactionManager tmgr = mock(TransactionManager.class);
//        
//    LogManager lmgr = new StagingLogManager(iomgr);
//    ActionManager amgr = mock(ActionManager.class);
//    
//    ReadManager rmgr = new ReadManagerImpl(iomgr);
//    Compactor c = mock(Compactor.class);
//    Configuration cfg = mock(Configuration.class);
    
    restart = RestartStoreFactory.createStore(omgr, temp, 4 * 1024 * 1024);
    restart.startup();
  }
  
  @After
  public void tearDown() {
  }
  // TODO add test methods here.
  // The methods must be annotated with annotation @Test. For example:
  //
  // @Test
  // public void hello() {}
  
  @Test
  public void testWrite() throws Throwable {
    restart.beginTransaction(true).put(byteBufferWithInt(1), byteBufferWithInt(2), byteBufferWithInt(3)).commit();
    long lsn = omgr.getLsn(byteBufferWithInt(1), byteBufferWithInt(2));
    Tuple<ByteBuffer,ByteBuffer,ByteBuffer> tuple = restart.get(lsn);
    Assert.assertTrue(tuple.getIdentifier().getInt() == 1);
    Assert.assertTrue(tuple.getKey().getInt() == 2);
    Assert.assertTrue(tuple.getValue().getInt() == 3);
    restart.beginTransaction(true).put(byteBufferWithInt(4), byteBufferWithInt(5), byteBufferWithInt(6)).commit();
    lsn = omgr.getLsn(byteBufferWithInt(4), byteBufferWithInt(5));
    tuple = restart.get(lsn);
    Assert.assertTrue(tuple.getIdentifier().getInt() == 4);
    Assert.assertTrue(tuple.getKey().getInt() == 5);
    Assert.assertTrue(tuple.getValue().getInt() == 6);
  }
  
    @Test
  public void testLoop() throws Throwable {
    int x = 0;
    while (x<100) {
        int id = x++;
        int key = x++;
        int value = x++;
        byte[] vc = new byte[1000];
        Arrays.fill(vc,(byte)(value & 0xff));
        restart.beginTransaction(true).put(byteBufferWithInt(id), byteBufferWithInt(key), ByteBuffer.wrap(vc)).commit();
        long lsn = omgr.getLsn(byteBufferWithInt(id), byteBufferWithInt(key));
        Tuple<ByteBuffer,ByteBuffer,ByteBuffer> tuple = restart.get(lsn);
        Assert.assertTrue(tuple.getIdentifier().getInt() == id);
        Assert.assertTrue(tuple.getKey().getInt() == key);
        Assert.assertTrue((tuple.getValue().get() & 0xff) == (value & 0xff));
        System.out.println(id + " " + key);
    }
  }
}
