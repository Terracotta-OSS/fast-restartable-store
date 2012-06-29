package com.terracottatech.frs;

import com.terracottatech.frs.object.heap.HeapObjectManager;
import com.terracottatech.frs.util.JUnitTestFolder;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Properties;

import static com.terracottatech.frs.util.TestUtils.byteBufferWithInt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

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
      HeapObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> heapObjectManager =
              new HeapObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>(1);
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = RestartStoreFactory.createStore(heapObjectManager,
                                                                                                      original, properties);
      restartStore.startup().get();

      for (int i = 0; i < 100; i++) {
        restartStore.beginTransaction(false).put(
                byteBufferWithInt(0), byteBufferWithInt(i), byteBufferWithInt(i)).commit();
      }

      restartStore.shutdown();
    }

    Backup.backup(original, copy);

    {
      HeapObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> heapObjectManager =
              spy(new HeapObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>(1));
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = RestartStoreFactory.createStore(heapObjectManager,
                                                                                                      copy, properties);
      restartStore.startup().get();

      for (int i = 0; i < 100; i++) {
        verify(heapObjectManager).replayPut(eq(byteBufferWithInt(0)), eq(byteBufferWithInt(i)),
                                            eq(byteBufferWithInt(i)), anyLong());
      }

      restartStore.shutdown();
    }
  }
}
