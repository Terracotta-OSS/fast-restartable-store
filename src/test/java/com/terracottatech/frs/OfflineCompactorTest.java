package com.terracottatech.frs;

import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.frs.object.SimpleRestartableMap;
import com.terracottatech.frs.util.JUnitTestFolder;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author tim
 */
public class OfflineCompactorTest {
  @Rule
  public JUnitTestFolder temporaryFolder = new JUnitTestFolder();

  @Test
  public void testBasicCompaction() throws Exception {
    File testFolder = temporaryFolder.newFolder("testCompaction");

    File uncompacted = new File(testFolder, "uncompacted");
    File compacted = new File(testFolder, "compacted");

    Properties properties = new Properties();
    properties.setProperty(FrsProperty.COMPACTOR_POLICY.shortName(),
                           "NoCompactionPolicy");
    properties.setProperty(FrsProperty.COMPACTOR_RUN_INTERVAL.shortName(), Integer.toString(Integer.MAX_VALUE));
    properties.setProperty(FrsProperty.COMPACTOR_START_THRESHOLD.shortName(), Integer.toString(Integer.MAX_VALUE));

    {
      assertThat(uncompacted.mkdirs(), is(true));
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager =
              new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> uncompactedStore =
              RestartStoreFactory.createStore(
                      objectManager,
                      uncompacted, properties);
      SimpleRestartableMap map =
              new SimpleRestartableMap(0, uncompactedStore,
                                       false);
      objectManager.registerObject(map);

      uncompactedStore.startup().get();

      for (int i = 0; i < 100; i++) {
        for (int j = 0; j < 100; j++) {
          map.put(Integer.toString(j), Integer.toString(i));
        }
      }

      uncompactedStore.shutdown();
      assertThat(objectManager.size(), is(100L));
    }

    new OfflineCompactor(uncompacted, compacted).compact();

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager =
              spy(new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>());
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> compactedStore =
              RestartStoreFactory.createStore(objectManager, compacted, properties);

      SimpleRestartableMap map =
              new SimpleRestartableMap(0, compactedStore,
                                       false);
      objectManager.registerObject(map);

      compactedStore.startup().get();

      for (int i = 0; i < 100; i++) {
        assertThat(map.get(Integer.toString(i)), is("99"));
      }

      compactedStore.shutdown();

      assertThat(objectManager.size(), is(100L));
      verify(objectManager, times(100)).replayPut(any(ByteBuffer.class),
                                                  any(ByteBuffer.class), any(ByteBuffer.class),
                                                  anyLong());
    }

    assertThat(FileUtils.sizeOfDirectory(compacted), lessThan(
            FileUtils.sizeOfDirectory(uncompacted)));
  }
}
