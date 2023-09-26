/*
 * Copyright (c) 2013-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
package com.terracottatech.frs;

import com.terracottatech.frs.config.FrsProperty;
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
import java.util.Properties;
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
  static Properties properties = new Properties();
  
  public RestartStoreReadTest() {
  }
  
  @BeforeClass
  public static void setUpClass() {
    properties = new Properties();
    properties.setProperty(FrsProperty.IO_RANDOM_ACCESS.shortName(), "true");
    properties.setProperty(FrsProperty.IO_NIO_SEGMENT_SIZE.shortName(), Integer.toString(4 * 1024));
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  @Before
  public void setUp() throws Throwable {
    File temp = folder.newFolder();

    omgr = new HeapObjectManager<ByteBuffer,ByteBuffer,ByteBuffer>(1);

    restart = RestartStoreFactory.createStore(omgr, temp,properties);
    restart.startup();
  }
  
  @After
  public void tearDown() throws Exception {
    restart.shutdown();
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
    if ( tuple instanceof Disposable ) {
      ((Disposable)tuple).dispose();
    }
    restart.beginTransaction(true).put(byteBufferWithInt(4), byteBufferWithInt(5), byteBufferWithInt(6)).commit();
    lsn = omgr.getLsn(byteBufferWithInt(4), byteBufferWithInt(5));
    tuple = restart.get(lsn);
    Assert.assertTrue(tuple.getIdentifier().getInt() == 4);
    Assert.assertTrue(tuple.getKey().getInt() == 5);
    Assert.assertTrue(tuple.getValue().getInt() == 6);
    if ( tuple instanceof Disposable ) {
      ((Disposable)tuple).dispose();
    }
  }
  
  @Test
  public void testLoop() throws Throwable {
    int x = 0;
    while (x<1000) {
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
        if ( tuple instanceof Disposable ) {
          ((Disposable)tuple).dispose();
        }
    }
  }
}
