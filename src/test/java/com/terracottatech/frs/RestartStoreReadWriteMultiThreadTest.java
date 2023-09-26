/*
 * Copyright (c) 2018-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.terracottatech.frs.util.TestUtils.byteBufferWithInt;

public class RestartStoreReadWriteMultiThreadTest {
  
  static final int DIFFERENT_KEYS = 8;
  static final int CACHE_ID = 1;
  static final int BATCH_SIZE = 3;
  static final int SYNC_SIZE = 100;
  static final int CONCURRENCY_COUNT = 4;
  static final int DATA_SIZE = 1000;
  static final int RECORD_COUNT = 20000;
  static final int ADDITIONAL_RECORD_COUNT = 100;
  
  private final AtomicBoolean stop = new AtomicBoolean(false);
  private final ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();

  @Rule
  public TemporaryFolder folder= new TemporaryFolder();

  private volatile ObjectManager<ByteBuffer,ByteBuffer,ByteBuffer> objectManager;
  private volatile RestartStore restartStore;
  
  
  public Properties setUpProperties() {
    Properties properties = new Properties();
    properties.setProperty(FrsProperty.IO_RANDOM_ACCESS.shortName(), Boolean.toString(true));
    properties.setProperty(FrsProperty.IO_NIO_SEGMENT_SIZE.shortName(), Integer.toString(1 * 1024));
    properties.setProperty(FrsProperty.COMPACTOR_SIZEBASED_THRESHOLD.shortName(), Double.toString(0.0d));
    return  properties;
  }

  @Before
  public void setUp() throws Throwable {
    Properties properties = setUpProperties();
    File temp = folder.newFolder();
    objectManager = new HeapObjectManager<>(CONCURRENCY_COUNT);
    restartStore = RestartStoreFactory.createStore(objectManager, temp,properties);
    restartStore.startup();
  }

  @After
  public void tearDown() throws Exception {
    restartStore.shutdown();
  }

  @Test
  public void testConcurrentReadWrite() throws Exception {
    List<Future> readerThreads = new ArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(DIFFERENT_KEYS);
    for(int i = 0; i < DIFFERENT_KEYS; i++){
      readerThreads.add(executorService.submit(
          ()->fetchRecords()
      ));
    }
    upsertRecords(RECORD_COUNT);
    //No problem even if it fails => Already set to false
    stop.compareAndSet(false, true);
    readerThreads.forEach(f -> {
      try {
        /*
        If fetching threads are not stopped already, keep updating records so that lsn keeps increasing.
        This stops the potential of deadlock as we are artificially searching for look-ahead lsn in 
        fetching threads. Otherwise, it might happen that fetching thread is waiting for a synced marker 
        greater than the lsn in NIOManager and the synced marker is never generated as we stopped producing
        records. Before we come here, stop flag is already set, so fetching threads will quickly finish.
         */
        while(!f.isDone()){
          upsertRecords(ADDITIONAL_RECORD_COUNT);
        }
        f.get();
      }
      catch (Exception ex){
        throw new RuntimeException(ex);
      }
    });
    //Sysout so that we know the exceptions generated
    exceptions.forEach(System.out::println);
    executorService.shutdown();
    Assert.assertTrue(exceptions.isEmpty());
  }
  
  private void fetchRecords() {
    Random randomKeyGenerator = new Random();
    while(!stop.get()){
      int key = randomKeyGenerator.nextInt(DIFFERENT_KEYS);
      boolean addLookAhead = randomKeyGenerator.nextBoolean();
      try {
        long lsn = objectManager.getLsn(byteBufferWithInt(CACHE_ID), byteBufferWithInt(key));
        if(addLookAhead){
          int lsnLookAhead = randomKeyGenerator.nextInt(101);
          lsn += lsnLookAhead;
        }
        if(lsn >= 0){
          restartStore.get(lsn);
        }
      }
      catch (IllegalArgumentException ex) {
        String message = ex.getMessage();
        if(message != null && message.startsWith("action is not a gettable event")){
          /*
          This is fine. We are trying to artificially increase the last visible
          lsn so that we can simulate the issue described in:
          https://itrac.eur.ad.sag/browse/TDB-3066
          
          When we randomly increase lsn, we get a random lsn which may return a metadata (e.g. commit log).
          In that case, we are going to get IllegalArgumentException with a message "action is not a gettable event"
           */
          continue;
        }
        exceptions.add(ex);
        //No problem even if it fails => Already set to false
        stop.compareAndSet(false, true);
      }
      catch (Exception ex) {
        exceptions.add(ex);
        //No problem even if it fails => Already set to false
        stop.compareAndSet(false, true);
      }
      catch (AssertionError ex) {
        String message = ex.getMessage();
        if(message != null && message.startsWith("Marker")){
          /*
          This is fine. We are trying to artificially increase the last visible
          lsn so that we can simulate the issue described in:
          https://itrac.eur.ad.sag/browse/TDB-3066
          
          When we randomly increase lsn, we get a random lsn which might not
          be a valid one which makes the code throw AssertionError starting with 
          the word "Marker"
           */
          continue;
        }
        exceptions.add(ex);
        //No problem even if it fails => Already set to false
        stop.compareAndSet(false, true);
      }
    }
  }
  
  private void upsertRecords(int numberOfIterations) throws Exception {
    byte[] vc = new byte[DATA_SIZE];
    Arrays.fill(vc,(byte)(0xff));
    int x = 0;
    Transaction trans = null;
    boolean transCommitStatus = false;
    int operationCount = 0;

    int batchSize = BATCH_SIZE;
    int nosyncCount = 0;
    while (x < numberOfIterations) {
      int key = x++ % DIFFERENT_KEYS;
      Thread.yield();
      if (operationCount == 0) {
        transCommitStatus = false;
        boolean doSync = (++nosyncCount % SYNC_SIZE) == 0;
        trans = restartStore.beginTransaction(doSync);
      }

      trans.put(byteBufferWithInt(CACHE_ID), byteBufferWithInt(key), ByteBuffer.wrap(vc));
      operationCount++;
      Thread.yield();
      if (operationCount == batchSize) {
        transCommitStatus = true;
        trans.commit();
        operationCount = 0;
      }
      Thread.yield();
    }
    if(!transCommitStatus){
      trans.commit();
    }
  }
}