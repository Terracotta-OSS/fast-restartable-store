/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import com.terracottatech.frs.Constants;
import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.MaskingBufferSource;
import com.terracottatech.frs.io.SplittingBufferSource;
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
    private static BufferSource src;
    
    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder();    
    
    public NIOMarkersTest() {
    }


    @BeforeClass
    public static void setUpClass() throws Exception {
      src = new MaskingBufferSource(new SplittingBufferSource(16, 8 * 1024 * 1024));
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    protected void setUp(NIOAccessMethod method) throws Exception {
        workArea = folder.newFolder();
        System.out.println(workArea.getAbsolutePath());
        this.method = method;
        manager = new NIOManager(workArea.getAbsolutePath(), method.toString(),null,  1 * 1024 * 1024, -1, -1, false, src);
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
        current = Constants.FIRST_LSN;
        min = Constants.FIRST_LSN;
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
        manager = new NIOManager(workArea.getAbsolutePath(), method.toString(),null, 1 * 1024 * 1024, -1, -1 , false, src);
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
