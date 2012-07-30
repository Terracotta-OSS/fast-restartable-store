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
import com.terracottatech.frs.util.JUnitTestFolder;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.junit.*;

/**
 *
 * @author mscott
 */
public class AbstractReadbackStrategyTest {
    File workArea;
    NIOManager manager; 
    long current;
    
    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder(); 
    
    public AbstractReadbackStrategyTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() throws Exception {
        workArea = folder.newFolder();
        System.out.println(workArea.getAbsolutePath());
        manager = new NIOManager(workArea.getAbsolutePath(), 1 * 1024 * 1024, 10 * 1024 * 1024);
        manager.setMinimumMarker(100);
    //  create a 10k lsn window
        for(int x=0;x<1000;x++) {
            writeChunkWithMarkers(10);
            manager.sync();
        }
    }
    
    private void writeChunkWithMarkers(int size) throws Exception {
        ArrayList<LogRecord> list = new ArrayList<LogRecord>();
        list.add(new LogRecordImpl(current, new ByteBuffer[] {ByteBuffer.allocate(1024)}, null));
        manager.write(new LogRegionPacker(Signature.NONE).pack(list),current+=size);
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
    
    public ReadbackStrategy testConsistency() throws Exception {
        NIOSegmentList list = new NIOSegmentList(workArea);
        final FileBuffer  buffer = new FileBuffer(new FileInputStream(list.getEndFile()).getChannel(), ByteBuffer.allocate((int)list.getEndFile().length()));
        buffer.read(1);
        buffer.skip(NIOSegmentImpl.FILE_HEADER_SIZE);
        ReadbackStrategy rs = new AbstractReadbackStrategy() {
            
            ByteBuffer[] hold = null;

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
        };
        
        
        while (rs.hasMore(Direction.FORWARD)) {
            Chunk c = rs.iterate(Direction.FORWARD);
        }
        
        return rs;
        
    }
    

}
