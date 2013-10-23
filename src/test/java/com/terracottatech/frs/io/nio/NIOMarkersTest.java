/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.WrappingChunk;
import com.terracottatech.frs.util.JUnitTestFolder;

import java.io.File;
import java.nio.ByteBuffer;
import org.junit.*;

/**
 *
 * @author mscott
 */
public abstract class NIOMarkersTest {
    File workArea;
    NIOManager manager;
    long current;
    long min;
    NIOAccessMethod method;
    
    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder();    
    
    public NIOMarkersTest() {
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
        this.method = method;
        manager = new NIOManager(workArea.getAbsolutePath(), method.toString(), 1 * 1024 * 1024, 10 * 1024 * 1024, false);
        manager.setMinimumMarker(100);
    }
    
    @Test
    public void testAsyncClean() throws Exception {
        current = 100;
        min = 100;
    //  create a 10k lsn window
        for(int x=0;x<1000;x++) {
            writeChunkWithMarkers(10);
        }
        min = current;
        manager.setMinimumMarker(min);
//  create a 10k lsn window
        for(int x=0;x<1000;x++) {
            writeChunkWithMarkers(10);
        }      
        manager.clean(0);
        NIOSegmentList list = new NIOSegmentList(workArea);
//  make sure files were deleted
        File first = list.getBeginningFile();
        System.err.println(first.getName());
        int seg = new NIOSegment(null,list.getBeginningFile()).getSegmentId();
        System.out.println(seg);
        assert(seg != 0);        
        
        testRecovery();
    }
    
    
    @Test
    public void testSyncClean() throws Exception {
        current = 100;
        min = 100;
    //  create a 10k lsn window
        for(int x=0;x<1000;x++) {
            writeChunkWithMarkers(10);
            manager.sync();
        }
        min = current;
        manager.setMinimumMarker(min);
//  create a 10k lsn window
        for(int x=0;x<1000;x++) {
            writeChunkWithMarkers(10);
            manager.sync();
        }      
        manager.clean(0);
        NIOSegmentList list = new NIOSegmentList(workArea);
//  make sure files were deleted
        File first = list.getBeginningFile();
        System.err.println(first.getName());
        int seg = new NIOSegment(null,list.getBeginningFile()).getSegmentId();
        System.out.println(seg);
        assert(seg != 0);           
        
        testRecovery();
    }    
    
    public void testRecovery() throws Exception {
        manager.close();
        manager = new NIOManager(workArea.getAbsolutePath(), method.toString(),1 * 1024 * 1024, 10 * 1024 * 1024, false);
        manager.seek(IOManager.Seek.END.getValue());
        Chunk c = manager.read(Direction.REVERSE);
        int count = 0;
        while ( c != null ) {
            count++;
            c = manager.read(Direction.REVERSE);
        }
        System.out.println(manager.getMinimumMarker());
        System.out.println(manager.getCurrentMarker());
        assert(manager.getMinimumMarker() == min);
        assert(manager.getCurrentMarker() == 20100);
        System.out.println("chunks after clean " + count);
        assert(count < 2000);
    }
    
    private void writeChunkWithMarkers(int size) throws Exception {
        manager.write(new WrappingChunk(ByteBuffer.allocate(10 * 1024)),current+=size);
    }
    
    @After
    public void tearDown() throws Exception {
        manager.close();
        manager = null;
        System.gc();
    }

}
