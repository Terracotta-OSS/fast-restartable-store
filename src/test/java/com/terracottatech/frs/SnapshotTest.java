package com.terracottatech.frs;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;

import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.frs.object.SimpleRestartableMap;
import com.terracottatech.frs.util.JUnitTestFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * @author tim
 */
public class SnapshotTest {
  @Rule
  public JUnitTestFolder tempFolder = new JUnitTestFolder();

  @Test
  public void testSimpleBackup() throws Exception {
    File backupTo = tempFolder.newFolder();
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = createStore(tempFolder.newFolder(), objectManager);

      Map<String, String> map = createMap(restartStore, objectManager);

      restartStore.startup().get();

      for (int i = 0; i < 1000; i++) {
        map.put(Integer.toString(i), "foo");
      }

      Snapshot snapshot = restartStore.snapshot();

      for (int i = 1000; i < 2000; i++) {
        map.put(Integer.toString(i), "foo");
      }

      map.clear();

      while (snapshot.hasNext()) {
        FileUtils.copyFileToDirectory(snapshot.next(), backupTo);
      }
      snapshot.close();

      restartStore.shutdown();
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = createStore(backupTo, objectManager);

      Map<String, String> map = createMap(restartStore, objectManager);

      restartStore.startup().get();

      for (int i = 0; i < 1000; i++) {
        assertThat(map.get(Integer.toString(i)), is("foo"));
      }

      for (int i = 1000; i < 2000; i++) {
        assertThat(map.get(Integer.toString(i)), nullValue());
      }

      restartStore.shutdown();
    }
  }

  @Test
  public void testMultiThreadedBackup() throws Exception {
    File backup1 = tempFolder.newFolder();
    File backup2 = tempFolder.newFolder();
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager1 = new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore1 = createStore(tempFolder.newFolder(), objectManager1, 128000000);
      final Map<String, String> map1 = createMap(restartStore1, objectManager1);

      restartStore1.startup().get();

      final AtomicBoolean done = new AtomicBoolean(false);

      ExecutorService executorService = Executors.newCachedThreadPool();
      List<Future<?>> futures = new ArrayList<Future<?>>();
      for (int i = 0; i < 15; i++) {
        final int index = i;
        futures.add(executorService.submit(new Runnable() {
          @Override
          public void run() {
            int i = 0;
            while (!done.get()) {
              map1.put(index + "-" + i, "a");
              i++;
            }
          }
        }));
      }

      Thread.sleep(1000);

      Snapshot snapshot1 = restartStore1.snapshot();

      done.set(true);

      for (Future<?> future : futures) {
        future.get();
      }

      List<File> files = new ArrayList<File>();
      while (snapshot1.hasNext()) {
        files.add(snapshot1.next());
      }
      copySnapshotTo(files.iterator(), backup1);

      for (int i = 0; i < 1000; i++) {
        map1.put("main-" + i, "a");
      }
      copySnapshotTo(files.iterator(), backup2);

      snapshot1.close();

      restartStore1.shutdown();
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager1 = new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager2 = new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore1 = createStore(backup1, objectManager1);
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore2 = createStore(backup2, objectManager2);
      Map<String, String> map1 = createMap(restartStore1, objectManager1);
      Map<String, String> map2 = createMap(restartStore2, objectManager2);

      restartStore1.startup().get();
      restartStore2.startup().get();

      assertThat("Snapshot is not stable.", map1.size(), equalTo(map2.size()));

      restartStore1.shutdown();
      restartStore2.shutdown();
    }

  }

  private static void copySnapshotTo(Iterator<File> snapshot, File toDir) throws IOException {
    while (snapshot.hasNext()) {
      FileUtils.copyFileToDirectory(snapshot.next(), toDir);
    }
  }
  private static RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createStore(File folder, ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, int segmentSize) throws Exception {
    Properties properties = new Properties();
    properties.put("io.nio.segmentSize", Integer.toString(segmentSize));
    return RestartStoreFactory.createStore(objectManager, folder, properties);
  }

  private static RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createStore(File folder, ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager) throws Exception {
    return createStore(folder, objectManager, 1024);
  }

  private static Map<String, String> createMap(RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore, RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager) {
    SimpleRestartableMap map = new SimpleRestartableMap(0, restartStore, false);
    objectManager.registerObject(map);
    return map;
  }
}
