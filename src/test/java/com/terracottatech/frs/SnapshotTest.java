package com.terracottatech.frs;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;

import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.frs.object.SimpleRestartableMap;
import com.terracottatech.frs.util.JUnitTestFolder;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
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
      Properties properties = new Properties();
      properties.put("io.nio.segmentSize", "1024");
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = RestartStoreFactory.createStore(objectManager, tempFolder.newFolder(), properties);

      SimpleRestartableMap map = new SimpleRestartableMap(0, restartStore, false);
      objectManager.registerObject(map);

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
      Properties properties = new Properties();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = RestartStoreFactory.createStore(objectManager, backupTo, properties);

      SimpleRestartableMap map = new SimpleRestartableMap(0, restartStore, false);
      objectManager.registerObject(map);

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
}
