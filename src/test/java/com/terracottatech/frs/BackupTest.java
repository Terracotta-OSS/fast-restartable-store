package com.terracottatech.frs;

import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.frs.object.SimpleRestartableMap;
import com.terracottatech.frs.util.JUnitTestFolder;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.terracottatech.frs.util.TestUtils.byteBufferWithInt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author tim
 */
public class BackupTest {
  @Rule
  public JUnitTestFolder tempFolder = new JUnitTestFolder();

  @Test
  public void testBasicBackup() throws Exception {
    File folder = tempFolder.newFolder("testBackup");

    File original = new File(folder, "original");
    File copy = new File(folder, "copy");

    Properties properties = new Properties();

    {
      assertThat(original.mkdirs(), is(true));
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager =
              new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();

      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = RestartStoreFactory.createStore(objectManager,
                                                                                                      original, properties);
      SimpleRestartableMap map = new SimpleRestartableMap(byteBufferWithInt(0), restartStore, false);
      objectManager.registerObject(map);

      restartStore.startup().get();

      for (int i = 0; i < 100; i++) {
        map.put(Integer.toString(i), Integer.toString(i));
      }

      restartStore.shutdown();
    }

    Backup.backup(original, copy);

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager =
              new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();

      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = RestartStoreFactory.createStore(objectManager,
                                                                                                      copy, properties);
      SimpleRestartableMap map = new SimpleRestartableMap(byteBufferWithInt(0), restartStore, false);
      objectManager.registerObject(map);
      restartStore.startup().get();

      for (int i = 0; i < 100; i++) {
        assertThat(map.get(Integer.toString(i)), is(Integer.toString(i)));
      }

      restartStore.shutdown();
    }
  }

  @Test
  public void testBackupWhileGeneratingGarbage() throws Exception {
    File folder = tempFolder.newFolder("testBackupWithCreatingGarbage");

    File original = new File(folder, "original");
    File copy = new File(folder, "copy");

    Properties properties = new Properties();
    properties.setProperty(FrsProperty.IO_NIO_SEGMENT_SIZE.shortName(), "8192");
    properties.setProperty(FrsProperty.COMPACTOR_START_THRESHOLD.shortName(), "120");

    assertThat(original.mkdirs(), is(true));

    {

      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> registerableObjectManager = new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();

      final RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = RestartStoreFactory.createStore(registerableObjectManager,
                                                                                                            original, properties);

      SimpleRestartableMap restartableMap = new SimpleRestartableMap(byteBufferWithInt(0), restartStore, false);
      registerableObjectManager.registerObject(restartableMap);

      restartStore.startup().get();

      for (int i = 0; i < 100; i++) {
        restartableMap.put(Integer.toString(i), Integer.toString(i));
      }

      restartStore.shutdown();
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> registerableObjectManager = new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();

      final RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = RestartStoreFactory.createStore(registerableObjectManager,
                                                                                                            original, properties);

      final SimpleRestartableMap restartableMap = new SimpleRestartableMap(byteBufferWithInt(0), restartStore, false);
      registerableObjectManager.registerObject(restartableMap);
      restartStore.startup().get();

      final AtomicBoolean done = new AtomicBoolean();
      final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
      Thread garbageGeneration = new Thread() {
        @Override
        public void run() {
          while (!done.get()) {
            try {
              restartableMap.put("0", "0");
            } catch (Throwable t) {
              error.set(t);
              break;
            }
          }
        }
      };

      garbageGeneration.start();

      Thread.sleep(5 * 1000);

      Backup.backup(original, copy);

      done.set(true);

      garbageGeneration.join();

      if (error.get() != null) {
        throw new AssertionError(error.get());
      }

      assertThat(restartableMap.size(), is(100));
      restartStore.shutdown();
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> registerableObjectManager = new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();

      final RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = RestartStoreFactory.createStore(registerableObjectManager,
                                                                                                            copy, properties);

      final SimpleRestartableMap restartableMap = new SimpleRestartableMap(byteBufferWithInt(0), restartStore, false);
      registerableObjectManager.registerObject(restartableMap);
      restartStore.startup().get();

      assertThat(restartableMap.size(), is(100));

      for (int i = 0; i < 100; i++) {
        assertThat(restartableMap.get(Integer.toString(i)), is(Integer.toString(i)));
      }

      restartStore.shutdown();
    }
  }
}
