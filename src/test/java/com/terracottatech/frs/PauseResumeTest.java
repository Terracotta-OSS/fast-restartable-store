package com.terracottatech.frs;

import org.apache.commons.io.FileUtils;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.frs.object.SimpleRestartableMap;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class PauseResumeTest {
  private static final int MAX_ITERATIONS = 10;

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();
  public Properties properties = new Properties();

  @Test
  public void testFullPeriodicBackup() throws Exception {
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = createStore(tmpFolder.newFolder(), objectManager);
      Map<String, String> map1 = createMap(restartStore, objectManager, 0);
      Map<String, String> map2 = createMap(restartStore, objectManager, 1);
      restartStore.startup().get();
      ExecutorService executor = Executors.newFixedThreadPool(5);

      CyclicBarrier barrier = new CyclicBarrier(3);
      Future<Void> backupFuture = doPeriodicBackup(executor, restartStore, barrier);
      Future<Void> map1Future = doPuts(executor, map1, barrier);
      Future<Void> map2Future = doPuts(executor, map2, barrier);

      backupFuture.get();
      map1Future.get();
      map2Future.get();
      assertThat(map1Future.isDone(), is(true));
      assertThat(map2Future.isDone(), is(true));
      assertThat(backupFuture.isDone(), is(true));

      // ensure compaction is happening post backup
      File f1, f2;
      {
        Snapshot snapshot = restartStore.snapshot();
        f1 = snapshot.iterator().next();
        snapshot.close();
      }

      barrier = new CyclicBarrier(2);
      for (int i = 0; i < 5; i++) {
        map1Future = doPuts(executor, map1, barrier);
        map2Future = doPuts(executor, map2, barrier);
        map1Future.get();
        map2Future.get();
      }

      {
        Snapshot snapshot = restartStore.snapshot();
        f2 = snapshot.iterator().next();
        snapshot.close();
      }

      assertThat(f1.getName().equals(f2.getName()), is(false));
      restartStore.shutdown();
    }
  }

  @Test
  public void testRestore() throws Exception {
    File restoreFrom;
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = createStore(tmpFolder.newFolder(), objectManager);
      Map<String, String> map1 = createMap(restartStore, objectManager, 0);
      Map<String, String> map2 = createMap(restartStore, objectManager, 1);
      restartStore.startup().get();

      doSomePuts(map1, 0, 10);
      doSomePuts(map2, 0, 100);

      // Pause and resume for backup
      restoreFrom = tmpFolder.newFolder();
      Future<Future<Snapshot>> snapshotFutureSquare = restartStore.pause();
      Future<Snapshot> snapshotFuture = snapshotFutureSquare.get();
      restartStore.resume();

      doSomePuts(map1, 1000, 1002);
      doSomePuts(map2, 10000, 10010);

      // now copy snapshot when ready
      Snapshot snapshot = snapshotFuture.get();
      for (File f : snapshot) {
        System.out.println("Copying File " + f.getAbsolutePath() + " to " + restoreFrom.getAbsolutePath());
        FileUtils.copyFileToDirectory(f, restoreFrom);
      }
      snapshot.close();

      doSomePuts(map1, 2000, 2005);
      doSomePuts(map2, 9900, 9999);

      restartStore.shutdown();
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = createStore(restoreFrom, objectManager);
      Map<String, String> map1 = createMap(restartStore, objectManager, 0);
      Map<String, String> map2 = createMap(restartStore, objectManager, 1);
      restartStore.startup().get();

      assertRestoredMap(map1, 0, 10, true);
      assertRestoredMap(map2, 0, 100, true);

      assertRestoredMap(map1, 1000, 1010, false);
      assertRestoredMap(map1, 2000, 2010, false);
      assertRestoredMap(map2, 9000, 10020, false);

      restartStore.shutdown();
    }
  }

  private void assertRestoredMap(Map<String, String> map, int startIdx, int endIdx, boolean exists) {
    for (int i = startIdx; i < endIdx; i++) {
      if (exists) {
        assertThat(map.get(Integer.toString(i)), is("foo" + i));
      } else {
        assertNull(map.get(Integer.toString(i)));
      }
    }
  }

  private void doSomePuts(Map<String, String> map, int startIdx, int endIdx) {
    for (int i = startIdx; i < endIdx; i++) {
      map.put(Integer.toString(i), "foo" + i);
    }
  }

  private Future<Void> doPuts(final ExecutorService executor,
                              final Map<String, String> thisMap,
                              final CyclicBarrier barrier) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        for (int k = 0; k < MAX_ITERATIONS; k++) {
          for (int i = k * 5; i < ((k + 1) * 5); i++) {
            thisMap.put(Integer.toString(i), "foo" + i);
          }
          barrier.await();
          for (int i = k * 1000; i < ((k + 1) * 1000); i++) {
            thisMap.put(Integer.toString(i), "foo" + i);
          }
          barrier.await();
          for (int i = (k * 1000) + 100; i < ((k + 1) * 1000); i++) {
            thisMap.remove(Integer.toString(i));
          }
          Thread.yield();
          Thread.yield();
        }
        return  null;
      }
    });
  }

  private Future<Void> doPeriodicBackup(ExecutorService executor,
                                        final RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore,
                                        final CyclicBarrier barrier) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        int backupAborted = 0;
        Random rand = new Random();
        for (int i = 0; i < MAX_ITERATIONS; i++) {
          File backupTo = tmpFolder.newFolder();
          barrier.await();
          Future<Future<Snapshot>> pauseForSnapshot = restartStore.pause();
          Future<Snapshot> snapshotFuture = pauseForSnapshot.get();
          Thread.yield();
          Thread.yield();
          boolean waitedInPause = false;
          if (rand.nextInt(100) > 80) {
            barrier.await();
            waitedInPause = true;
          }
          try {
            restartStore.resume();
            Snapshot snapshot = snapshotFuture.get();
            for ( File f : snapshot ) {
              System.out.println("Copying File " + f.getAbsolutePath() + " to " + backupTo.getAbsolutePath());
              FileUtils.copyFileToDirectory(f, backupTo);
            }
            snapshot.close();
          } catch (NotPausedException e) {
            snapshotFuture.get().close();
            backupAborted++;
          }
          if (!waitedInPause) {
            barrier.await();
          }
        }
        assertThat(backupAborted, Matchers.lessThan(MAX_ITERATIONS));
        System.out.println("Backup Aborted " + backupAborted + " times");
        return null;
      }
    });
  }

  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createStore(
      File folder,
      ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
      int segmentSize) throws Exception {
    properties.put("io.nio.segmentSize", Integer.toString(segmentSize));
    properties.put("compactor.runInterval", Long.toString(1L));
    return RestartStoreFactory.createStore(objectManager, folder, properties);
  }

  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createStore(
      File folder,
      ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager) throws Exception {
    return createStore(folder, objectManager, 1024);
  }

  private static Map<String, String> createMap(RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore,
                                               RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                                               int identifier) {
    SimpleRestartableMap map = new SimpleRestartableMap(identifier, restartStore, false);
    objectManager.registerObject(map);
    return map;
  }
}