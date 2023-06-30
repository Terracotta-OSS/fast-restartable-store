/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.frs;

import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.frs.object.SimpleRestartableMap;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * End to end test for FRS {@link RestartStore#freeze()} functionality
 */
public class RestartStoreFreezeTest {
  private static final int NUM_MAPS = 4;
  private static final int SEGMENT_SIZE_KB = 16;
  private static final int NUM_ITEMS_IN_MAP1 = 10;
  private static final int NUM_ITEMS_IN_MAP2 = 20;
  private static final int PER_ENTRY_SIZE_KB = 1;

  private final Properties properties = new Properties();
  private final ExecutorService executorService = Executors.newFixedThreadPool(4);

  public RestartStoreFreezeTest() {
    properties.setProperty(FrsProperty.IO_NIO_SEGMENT_SIZE.shortName(), Integer.toString(SEGMENT_SIZE_KB * 1024));
  }

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Ignore
  @Test
  public void testFreezeSynchronousWithoutShutdown() throws Exception {
    runTest(true);
  }

  @Ignore
  @Test
  public void testFreezeAsynchronousWithoutShutdown() throws Exception {
    runTest(false);
  }

  @Test
  public void testFreezeAsynchronousWithShutdownAborted() throws Exception {
    File frsFolder = folder.newFolder();
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restart = loadValuesAndFreeze(frsFolder, false);
    restart.resume();
    executorService.shutdown();
    executorService.awaitTermination(40, TimeUnit.SECONDS);
    restart.shutdown();
    assertStoredNewValues(frsFolder, false);
  }

  @Test
  public void testFreezeWhenRestartStoreNotStarted() throws Exception {
    File frsFolder = folder.newFolder();
    Map<Integer, SimpleRestartableMap> mapList = new HashMap<>();
    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> omgr = new RegisterableObjectManager<>();
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restart = RestartStoreFactory.createStore(omgr, frsFolder, properties);
    prepareMaps(restart, omgr, mapList, false);
    restart.freeze();
    restart.resume();
    restart.startup().get();
    mapList.get(1).put("1", "1");
    assertThat(mapList.get(1).get("1"), is("1"));
    restart.shutdown();
  }

  private void runTest(boolean synchronous) throws Exception {
    File frsFolder = folder.newFolder();
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restart = loadValuesAndFreeze(frsFolder, synchronous);
    File newFrsFolder = folder.newFolder();
    copyFrsSegments(frsFolder, newFrsFolder);

    executorService.shutdownNow();
    executorService.awaitTermination(20, TimeUnit.SECONDS);
    assertStoredValues(newFrsFolder, synchronous);
    restart.shutdown();
  }

  private void copyFrsSegments(File fromFrsFolder, File toFrsFolder) throws Exception {
    String[] segFiles = fromFrsFolder.list((dir, name) -> name.startsWith("seg"));
    assertNotNull(segFiles);
    assertThat(segFiles.length, greaterThan(0));
    for (String seg : segFiles) {
      FileUtils.copyFileToDirectory(new File(fromFrsFolder, seg), toFrsFolder);
    }
  }

  private char getChar(char startChar, int addVal) {
    return ((char) (startChar + (addVal % 'z' - '0')));
  }

  private void tryWriteAfterFreeze(final SimpleRestartableMap map1, final SimpleRestartableMap map2) {
    // try to write post freeze
    executorService.submit(() -> {
      for (int i = 0; i < NUM_ITEMS_IN_MAP1 / 2; i++) {
        // post freeze..this should just hang until resumed
        map1.put("1k" + i, "TEST");
      }
      map2.remove("2k0");
    });
  }

  private void tryWriteBeforeFreeze(final int index, final SimpleRestartableMap map1, final SimpleRestartableMap map2) {
    // try to write before freeze
    final int map1Multiplier = (index + 1) * NUM_ITEMS_IN_MAP1;
    final int map2Multiplier = (index + 1) * NUM_ITEMS_IN_MAP2;
    executorService.submit(() -> {
      for (int i = 0; i < NUM_ITEMS_IN_MAP1 * 2; i++) {
        // post freeze..this should just hang
        map1.put("1k" + i + map1Multiplier, buildLargeString(1, '0'));
      }
      for (int i = 0; i < NUM_ITEMS_IN_MAP2 * 2; i++) {
        // post freeze..this should just hang
        map2.put("1k" + map2Multiplier + i, buildLargeString(2,'0'));
      }
    });
  }

  private void assertStoredValues(File frsFolder, boolean synchronous) throws Exception {
    Map<Integer, SimpleRestartableMap> mapList = new HashMap<>();
    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> omgr = new RegisterableObjectManager<>();
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restart = RestartStoreFactory.createStore(omgr, frsFolder, properties);
    prepareMaps(restart, omgr, mapList, synchronous);
    restart.startup().get();
    SimpleRestartableMap map1 = mapList.get(1);
    SimpleRestartableMap map2 = mapList.get(2);
    assertThat(map1.size(), greaterThanOrEqualTo(NUM_ITEMS_IN_MAP1/2));
    assertThat(map2.size(), greaterThanOrEqualTo(NUM_ITEMS_IN_MAP2));
    for (int k = 0; k < NUM_ITEMS_IN_MAP1/2; k++) {
      assertThat(map1.get("1k" + k), is(buildLargeString(PER_ENTRY_SIZE_KB, getChar('0', k))));
    }
    for (int k = NUM_ITEMS_IN_MAP1/2; k < NUM_ITEMS_IN_MAP1; k++) {
      assertNull(map1.get("1k" + k));
    }
    for (int k = 0; k < NUM_ITEMS_IN_MAP2; k++) {
      assertThat(map2.get("2k" + k), is(buildLargeString(PER_ENTRY_SIZE_KB, getChar('1', k))));
    }
    restart.shutdown();
  }

  private void assertStoredNewValues(File frsFolder, boolean synchronous) throws Exception {
    Map<Integer, SimpleRestartableMap> mapList = new HashMap<>();
    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> omgr = new RegisterableObjectManager<>();
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restart = RestartStoreFactory.createStore(omgr, frsFolder, properties);
    prepareMaps(restart, omgr, mapList, synchronous);
    restart.startup().get();
    SimpleRestartableMap map1 = mapList.get(1);
    for (int i = 0; i < NUM_ITEMS_IN_MAP1 / 2; i++) {
      assertThat("TEST" + i + " " + map1.size(), map1.get("1k" + i), is("TEST"));
    }
    restart.shutdown();
  }

  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> loadValuesAndFreeze(File frsFolder, boolean synchronous) throws Exception {
    Map<Integer, SimpleRestartableMap> mapList = new HashMap<>();
    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> omgr = new RegisterableObjectManager<>();
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restart = RestartStoreFactory.createStore(omgr, frsFolder, properties);
    prepareMaps(restart, omgr, mapList, synchronous);
    restart.startup().get();
    final SimpleRestartableMap map1 = mapList.get(1);
    final SimpleRestartableMap map2 = mapList.get(2);
    tryWriteBeforeFreeze(0, map1, map2);
    tryWriteBeforeFreeze(1, map1, map2);

    for (int k = 0; k < NUM_ITEMS_IN_MAP1; k++) {
      map1.put("1k" + k, buildLargeString(PER_ENTRY_SIZE_KB, getChar('0', k)));
    }
    for (int k = NUM_ITEMS_IN_MAP1/2; k < NUM_ITEMS_IN_MAP1; k++) {
      map1.remove("1k" + k);
    }
    for (int k = 0; k < NUM_ITEMS_IN_MAP2; k++) {
      map2.put("2k" + k, buildLargeString(PER_ENTRY_SIZE_KB, getChar('1', k)));
    }
    Thread.yield(); Thread.yield(); Thread.yield();
    Future<Void> postFreeze = restart.freeze().get();
    tryWriteAfterFreeze(map1, map2);
    // wait for all frozen data to be flushed
    postFreeze.get();
    return restart;
  }

  private String buildLargeString(int sizeInKB, char fillerValue) {
    char[] filler = new char[sizeInKB * 1024];
    Arrays.fill(filler, fillerValue);
    return new String(filler);
  }

  private void prepareMaps(RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restart,
                           RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> omgr,
                           Map<Integer, SimpleRestartableMap> mapOfMaps,
                           boolean synchronous) {
    for (int k = 1; k <= NUM_MAPS; k++) {
      SimpleRestartableMap map = new SimpleRestartableMap(k, restart, synchronous);
      mapOfMaps.put(k, map);
      omgr.registerObject(map);
    }
  }
}
