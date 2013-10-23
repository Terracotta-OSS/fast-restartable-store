/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.FileBuffer;
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

/**
 *
 * @author mscott
 */
public abstract class AbstractReadbackStrategyLegacyTest {
    File workArea;
    NIOManager manager; 
    long current = 100;
    
    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder(); 
    
    public AbstractReadbackStrategyLegacyTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    protected void setUp(NIOAccessMethod method) throws Exception {
        workArea = folder.newFolder();
        System.out.println(workArea.getAbsolutePath());
        manager = new NIOManager(workArea.getAbsolutePath(),method.toString(),  4 * 1024 * 1024, 10 * 1024 * 1024, false);
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
        manager.write(new LogRegionPacker(Signature.NONE).pack(list),current);
    }
    
    private void writePunyChunkWithMarkers(int size) throws Exception {
        ArrayList<LogRecord> list = new ArrayList<LogRecord>();
        list.add(new LogRecordImpl(new ByteBuffer[] {ByteBuffer.allocate(1)}, null));
        current+=size;
        manager.write(new LogRegionPacker(Signature.NONE).pack(list),current);
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
        ArrayList<Long> jumps = rs.readJumpList(endbuffer.getBuffers()[0]);
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
            count += jumps.size();
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

            ArrayList<Long> jumps = rs.readJumpList(buffer.getBuffers()[0]);
            if ( jumps == null ) {
                final long LAST_INT_WORD_IN_CHUNK = buffer.length()-ByteBufferUtils.INT_SIZE;
                final long LAST_SHORT_WORD_BEFORE_JUMP_MARK = LAST_INT_WORD_IN_CHUNK - ByteBufferUtils.SHORT_SIZE;
                int numberOfChunks = buffer.getShort(LAST_SHORT_WORD_BEFORE_JUMP_MARK);
                assert(numberOfChunks < 0);
                return;
            }
            count += jumps.size();
            System.out.println(jumps.size());
            buffer.close();
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
