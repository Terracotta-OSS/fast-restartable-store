package com.terracottatech.frs.recovery;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.log.LogRegionPacker;
import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.frs.object.SimpleRestartableMap;
import com.terracottatech.frs.util.JUnitTestFolder;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LogFormatCompatibilityTest extends AbstractRecoveryManagerImplTest {

  @Rule
  public JUnitTestFolder folder = new JUnitTestFolder();

  @Test
  public void testRecoverOldFormatLog() throws Exception {
    Path dbHome = folder.newFolder().toPath();
    Path segment = new File(LogFormatCompatibilityTest.class.getResource("/logs/old/seg000000000.frs").toURI()).toPath();
    Files.copy(segment, dbHome.resolve(segment.getFileName()));

    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = RestartStoreFactory.createStore(objectManager, dbHome.toFile(), new Properties());
    SimpleRestartableMap map = new SimpleRestartableMap(0, restartStore, false);
    objectManager.registerObject(map);
    restartStore.startup().get();
    try {
      for (int i = 0; i < 100; i++) {
        assertEquals(map.get(Integer.toString(i)), Integer.toString(i + 1));
      }
    } finally {
      restartStore.shutdown();
    }
  }

  @Test
  public void testRecoverMisidentifiedLogWithoutHelp() throws Exception {
    Path dbHome = folder.newFolder().toPath();
    Path segment = new File(LogFormatCompatibilityTest.class.getResource("/logs/misidentified/seg000000000.frs").toURI()).toPath();
    Files.copy(segment, dbHome.resolve(segment.getFileName()));

    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = RestartStoreFactory.createStore(objectManager, dbHome.toFile(), new Properties());
    SimpleRestartableMap map = new SimpleRestartableMap(0, restartStore, false);
    objectManager.registerObject(map);
    try {
      restartStore.startup();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      //expected failure
    }
  }

  @Test
  public void testRecoverMisidentifiedLogWithHelp() throws Exception {
    Path dbHome = folder.newFolder().toPath();
    Path segment = new File(LogFormatCompatibilityTest.class.getResource("/logs/misidentified/seg000000000.frs").toURI()).toPath();
    Files.copy(segment, dbHome.resolve(segment.getFileName()));

    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();
    Properties properties = new Properties();
    properties.put(FrsProperty.FORCE_LOG_REGION_FORMAT.shortName(), LogRegionPacker.NEW_REGION_FORMAT_STRING);
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = RestartStoreFactory.createStore(objectManager, dbHome.toFile(), properties);
    SimpleRestartableMap map = new SimpleRestartableMap(0, restartStore, false);
    objectManager.registerObject(map);
    restartStore.startup().get();
    try {
      for (int i = 0; i < 100; i++) {
        assertEquals(map.get(Integer.toString(i)), Integer.toString(i + 1));
      }
    } finally {
      restartStore.shutdown();
    }
  }
}
