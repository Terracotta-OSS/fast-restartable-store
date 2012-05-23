package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.*;
import com.terracottatech.frs.log.BufferListWrapper;
import com.terracottatech.frs.util.TestFolder;

import org.junit.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author mscott
 */
public class NIOStreamImplTest {
  private static final long CHUNK_OVERHEAD   = 32;
  private static final long MAX_SEGMENT_SIZE = 10 * 1024 * 1024;
  NIOStreamImpl stream;
  File          workArea;
  Random        r;

  @Rule
  public TestFolder folder = new TestFolder();

  @Before
  public void setUp() throws Exception {
    workArea = folder.newFolder();
    stream = new NIOStreamImpl(workArea, MAX_SEGMENT_SIZE, MAX_SEGMENT_SIZE * 10);
    long seed = System.currentTimeMillis();
    r = new Random(seed);
  }

  @After
  public void tearDown() throws Exception {
    stream.close();
    stream = null;
    System.gc();
  }

  /**
   * Test of append method, of class NIOStreamImpl.
   */
  @Test
  public void testAppend() throws Exception {
    assertThat(stream.append(newChunk(1)), is(CHUNK_OVERHEAD + 1));
    assertThat(listFiles().length, is(1));

    assertThat(stream.append(newChunk(MAX_SEGMENT_SIZE - 2)),
               is(CHUNK_OVERHEAD + MAX_SEGMENT_SIZE - 2));
    assertThat(listFiles().length, is(1));

    assertThat(stream.append(newChunk(1)), is(CHUNK_OVERHEAD + 1));
    assertThat(listFiles().length, is(2));
  }

  @Test
  public void testRead() throws Exception {
    long size = 30 * 1024 * 1024;
    int numChunks = 0;
    while (size > 0) {
      int s = r.nextInt((int) (size + 1));
      stream.append(newChunk(s));
      size -= s;
      numChunks++;
    }
    stream.close();

    NIOStreamImpl nioStream = new NIOStreamImpl(workArea, MAX_SEGMENT_SIZE, MAX_SEGMENT_SIZE * 10);
    nioStream.seek(-1);
    int foundChunks = 0;
    while (nioStream.read(Direction.REVERSE) != null) {
      foundChunks++;
    }
    assertThat(foundChunks, is(numChunks));
  }

  private File[] listFiles() {
    return workArea.listFiles(NIOSegmentList.SEGMENT_FILENAME_FILTER);
  }

  private Chunk newChunk(long size) {
    List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
    while (size > 0) {
      int s = r.nextInt((int) (size + 1));
      buffers.add(ByteBuffer.allocate(s));
      size -= s;
    }
    return new BufferListWrapper(buffers);
  }

  /**
   * Test of sync method, of class NIOStreamImpl.
   */
  @Test
  @Ignore
  public void testSync() throws Exception {
    System.out.println("sync");
    Chunk c = new WrappingChunk(ByteBuffer.allocateDirect(1024));
    stream.append(c);
    stream.sync();
    File lock = new File(workArea.getAbsolutePath() + "/FRS.lck");
    assertTrue(lock.exists());
    FileBuffer chunk = new FileBuffer(new FileInputStream(lock).getChannel(),ByteBuffer.allocate((int)lock.length()));
    System.out.format("uuid: %s segment: %d position: %d",
                      new UUID(chunk.getLong(), chunk.getLong()).toString(),
                      chunk.getInt(), chunk.getLong());
  }
  
  @Test
  public void testOpen() throws Exception {
    long size = 30 * 1024 * 1024;
    int numChunks = 0;
    while (size > 0) {
      int s = r.nextInt((int) (size + 1));
      stream.append(newChunk(s));
      size -= s;
      numChunks++;
    }
    stream.close();
    BufferSource bufs = new ManualBufferSource(1024 * 1024);
    NIOSegmentList list = new NIOSegmentList(workArea);
    list.setReadPosition(-1);
    new NIOSegmentImpl(stream,list.appendFile()).openForWriting(bufs).close();
    new NIOSegmentImpl(stream,list.appendFile()).openForWriting(bufs).close();
    new NIOSegmentImpl(stream,list.appendFile()).openForWriting(bufs).close();
    stream = new NIOStreamImpl(workArea, 10*1024*1024);
    stream.open();
    stream.seek(IOManager.Seek.END.getValue());
    Chunk c = stream.read(Direction.REVERSE);
    
  }
  
    
  @Test
  public void testMiniBuffers() throws Exception {
    stream.append(newChunk(40));

    stream.close();
    BufferSource bufs = new ManualBufferSource(1024 * 1024);
    NIOSegmentList list = new NIOSegmentList(workArea);
    list.setReadPosition(0);
    System.out.println("file length: " + list.getBeginningFile().length());
    new NIOSegmentImpl(stream,list.getBeginningFile()).openForReading(bufs).close();
    stream = new NIOStreamImpl(workArea, 10*1024*1024);
    stream.open();
    stream.seek(IOManager.Seek.END.getValue());
    Chunk c = stream.read(Direction.REVERSE);
    assertThat((int)c.remaining(),is(40));
  }
  
  @Test
  public void testMegaBuffers() throws Exception {
    stream.append(newChunk(10 * 1024 * 1024));

    stream.close();
    BufferSource bufs = new ManualBufferSource(1024 * 1024);
    NIOSegmentList list = new NIOSegmentList(workArea);
    list.setReadPosition(0);
    System.out.println("file length: " + list.getBeginningFile().length());
    try {
    new NIOSegmentImpl(stream,list.getBeginningFile()).openForReading(bufs).close();
    stream = new NIOStreamImpl(workArea, 10*1024*1024);
    stream.open();
    stream.seek(IOManager.Seek.END.getValue());
    Chunk c = stream.read(Direction.REVERSE);
    assertThat((int)c.remaining(),is(10 * 1024 * 1024));
    } catch ( IOException ioe ) {
        // likely to happen, check for no direct memory
        assertThat(ioe.getMessage(),is("no direct memory space"));
    }
  }  
}
