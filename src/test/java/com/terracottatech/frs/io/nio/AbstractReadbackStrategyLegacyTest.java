/*
 * Copyright (c) 2013-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.FileBuffer;
import com.terracottatech.frs.io.MaskingBufferSource;
import com.terracottatech.frs.io.SplittingBufferSource;
import com.terracottatech.frs.io.WrappingChunk;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.log.LogRecordImpl;
import com.terracottatech.frs.log.LogRegionPacker;
import com.terracottatech.frs.log.Signature;
import com.terracottatech.frs.util.ByteBufferUtils;
import com.terracottatech.frs.util.JUnitTestFolder;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import org.junit.*;

import static com.terracottatech.frs.config.FrsProperty.FORCE_LOG_REGION_FORMAT;

/**
 *
 * @author mscott
 */
public abstract class AbstractReadbackStrategyLegacyTest {
    File workArea;
    NIOManager manager; 
    long current = 100;
    private static BufferSource   src;
    
    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder(); 
    
    public AbstractReadbackStrategyLegacyTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
      src = new MaskingBufferSource(new SplittingBufferSource(32, 8* 1024*1024));
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    protected void setUp(NIOAccessMethod method) throws Exception {
        workArea = folder.newFolder();
        System.out.println(workArea.getAbsolutePath());
        manager = new NIOManager(workArea.getAbsolutePath(),method.toString(), null, 4 * 1024 * 1024, -1, -1, false, src);
        manager.setMinimumMarker(100);
    //  create a 10k lsn window
        for(int x=0;x<1000;x++) {
            writeChunkWithMarkers(10);
            manager.sync();
        }
    }
    
    private void writeChunkWithMarkers(int size) throws Exception {
        ArrayList<LogRecord> list = new ArrayList<LogRecord>();
        list.add(new LogRecordImpl(new ByteBuffer[] {ByteBuffer.allocate(1024)}, null));
        current += size;
        manager.write(new LogRegionPacker(Signature.NONE, null).pack(list),current);
    }
    
    private void writePunyChunkWithMarkers(int size) throws Exception {
        ArrayList<LogRecord> list = new ArrayList<LogRecord>();
        list.add(new LogRecordImpl(new ByteBuffer[] {ByteBuffer.allocate(1)}, null));
        current+=size;
        manager.write(new LogRegionPacker(Signature.NONE, (String) FORCE_LOG_REGION_FORMAT.defaultValue()).pack(list),current);
    }   
        
    
    @After
    public void tearDown() {
    }


    @Test
    public void testNotClosed() throws Exception {
        ReadbackStrategy rs = testConsistency();
        assert(rs.isConsistent() == false);
    }
    
    @Test
    public void testClosed() throws Exception {
        manager.close();
        ReadbackStrategy rs = testConsistency();
        assert(rs.isConsistent() == true);
    }    
    
    @Test
    public void testJumpList() throws Exception {
        NIOSegmentList list = new NIOSegmentList(workArea);
//  test non closed stream
        File end = list.getEndFile();
        final FileBuffer  endbuffer = new FileBuffer(new FileInputStream(end).getChannel(), ByteBuffer.allocate((int)end.length()));
        AbstractReadbackStrategy rs = new MockReadbackStrategy(endbuffer);
        long[] jumps = rs.readJumpList(endbuffer.getBuffers()[0]);
        assert(jumps == null);
        
        manager.close();
//  test non closed stream
        Iterator<File> fi = list.iterator();
        int count = 0;
        while ( fi.hasNext() ) {
            File file = fi.next();
            final FileBuffer  buffer = new FileBuffer(new FileInputStream(file).getChannel(), ByteBuffer.allocate((int)file.length()));
            buffer.read(1);
            buffer.skip(NIOSegment.FILE_HEADER_SIZE);
            rs = new MockReadbackStrategy(buffer);

            jumps = rs.readJumpList(buffer.getBuffers()[0]);
            count += jumps.length;
            buffer.close();
        }
        
        assert(count == 1000);
    }
    
    @Test
    public void testMoreThanShortWidthChunks() throws Exception {
    //  create a 10k lsn window
        for(int x=0;x<Short.MAX_VALUE*2 + 1;x++) {
            writePunyChunkWithMarkers(10);
            manager.sync();
        }
        manager.close();
//  test non closed stream
        NIOSegmentList list = new NIOSegmentList(workArea);
        Iterator<File> fi = list.iterator();
        int count = 0;
        while ( fi.hasNext() ) {
            File file = fi.next();
            final FileBuffer  buffer = new FileBuffer(new FileInputStream(file).getChannel(), ByteBuffer.allocate((int)file.length()));
            buffer.read(1);
            buffer.skip(NIOSegment.FILE_HEADER_SIZE);
            MockReadbackStrategy rs = new MockReadbackStrategy(buffer);

            long[] jumps = rs.readJumpList(buffer.getBuffers()[0]);
            if ( jumps != null ) {
                final long LAST_INT_WORD_IN_CHUNK = buffer.length()-ByteBufferUtils.INT_SIZE;
                final long LAST_INT_WORD_BEFORE_JUMP_MARK = LAST_INT_WORD_IN_CHUNK - ByteBufferUtils.INT_SIZE;
                int numberOfChunks = buffer.getInt(LAST_INT_WORD_BEFORE_JUMP_MARK);
                Assert.assertEquals(jumps.length, numberOfChunks);
                count += jumps.length;
                System.out.println(jumps.length);
                buffer.close();
                return;
            } else {
              throw new AssertionError();
            }

        }
        
        Assert.fail("filled up a segment with more than short width of chunks and still got jump lists");
        
    }
    
    private ReadbackStrategy testConsistency() throws Exception {
        NIOSegmentList list = new NIOSegmentList(workArea);
        final FileBuffer  buffer = new FileBuffer(new FileInputStream(list.getEndFile()).getChannel(), ByteBuffer.allocate((int)list.getEndFile().length()));
        buffer.read(1);
        buffer.skip(NIOSegment.FILE_HEADER_SIZE);
        ReadbackStrategy rs = new MockReadbackStrategy(buffer);
        
        while (rs.hasMore(Direction.FORWARD)) {
            Chunk c = rs.iterate(Direction.FORWARD);
        }
        
        buffer.close();
        return rs;
        
    }
    
    
    static class MockReadbackStrategy extends AbstractReadbackStrategy {
            
            private ByteBuffer[] hold = null;
            private final Chunk buffer;
            
            MockReadbackStrategy(Chunk src) {
                buffer = src;
            }
            
            @Override
            public long size() {
                return buffer.length();
            }

            @Override
            public boolean isConsistent() {
              return super.isCloseDetected();
            }

            @Override
            public long getMaximumMarker() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            
            @Override
            public Chunk scan(long marker) throws IOException {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
            
            @Override
            public Chunk iterate(Direction dir) throws IOException {
                try {
                    return new WrappingChunk(hold);
                } finally {
                    hold = null;
                }
            }

            @Override
            public boolean hasMore(Direction dir) throws IOException {
                hold = readChunk(buffer);
                return hold != null;
            }
    } 

}
