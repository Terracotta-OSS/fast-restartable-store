package com.terracottatech.frs;

import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.heap.HeapObjectManager;
import com.terracottatech.frs.util.JUnitTestFolder;
import com.terracottatech.frs.util.TestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
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

    {
      assertThat(uncompacted.mkdirs(), is(true));
      ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager =
              new HeapObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>(1);
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> uncompactedStore =
              RestartStoreFactory.createStore(
                      objectManager,
                      uncompacted, properties);
      uncompactedStore.startup().get();

      for (int i = 0; i < 100; i++) {
        for (int j = 0; j < 100; j++) {
          uncompactedStore.beginTransaction(false).put(TestUtils.byteBufferWithInt(0),
                                                       TestUtils.byteBufferWithInt(j),
                                                       TestUtils.byteBufferWithInt(
                                                               i)).commit();
        }
      }

      uncompactedStore.shutdown();
      assertThat(objectManager.size(), is(100L));
    }

    new OfflineCompactor(uncompacted, compacted).compact();

    {
      ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager =
              spy(new HeapObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>(1));
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> compactedStore =
              RestartStoreFactory.createStore(objectManager, compacted, properties);
      compactedStore.startup().get();
      compactedStore.shutdown();

      for (int i = 0; i < 100; i++) {
        verify(objectManager).replayPut(eq(TestUtils.byteBufferWithInt(0)),
                                        eq(TestUtils.byteBufferWithInt(i)),
                                        eq(TestUtils.byteBufferWithInt(99)),
                                        anyLong());
      }

      assertThat(objectManager.size(), is(100L));
    }

    assertThat(FileUtils.sizeOfDirectory(compacted), lessThan(
            FileUtils.sizeOfDirectory(uncompacted)));
  }
}
