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
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.io.*;
import com.terracottatech.frs.util.JUnitTestFolder;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import org.junit.*;

/**
 *
 * @author mscott
 */
@Ignore
public class NIOCorruptionTests {
    File workArea;
    BufferBuilder  builder;
    NIOManager manager;
    long current;
    long min;
    private static BufferSource src;
    private static final int DATA_SIZE = 10 * 1024;
    
    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder();        
    
    public NIOCorruptionTests() {
        
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
      src = new MaskingBufferSource(new SplittingBufferSource(16, 8 * 1024 * 1024));
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        
    }
    
    @Before
    public void setUp() throws Exception {
        workArea = folder.newFolder();
        Properties props = new Properties();
        props.setProperty(FrsProperty.IO_NIO_SEGMENT_SIZE.shortName(), Integer.toString(1 * 1024 * 1024));
        props.setProperty(FrsProperty.IO_NIO_POOL_MEMORY_SIZE.shortName(), Integer.toString(10 * 1024 * 1024));
//        props.setProperty("io.nio.bufferBuilder", "com.terracottatech.frs.io.nio.CorruptionBuilder");
        props.store(new FileWriter(new File(workArea,Configuration.USER_PROPERTIES_FILE)), "frs test properties");
        
        Configuration config = Configuration.getConfiguration(workArea);
        manager = new NIOManager(config,src);
        manager.setMinimumMarker(Constants.FIRST_LSN);
        current = Constants.FIRST_LSN;
    }
    
    @Test
    public void testZeroSegmentException() throws Exception {
        manager.setBufferBuilder(new ExceptionBuilder(0));
        long written = 0;
        try {
            while ( true ) {
                manager.write(new WrappingChunk(ByteBuffer.allocate(DATA_SIZE)), current++);
                written += 1;
            }
        } catch ( IOException ioe ) {
            System.out.println("written " + written);
        }
        assert(testRecovery() == (written * DATA_SIZE));
    }  
    
    @Test
    public void testOneSegmentException() throws Exception {
        manager.setBufferBuilder(new ExceptionBuilder(300 * 1024));
        long written = 0;
        try {
            while ( true ) {
                manager.write(new WrappingChunk(ByteBuffer.allocate(DATA_SIZE)), current++);
                written += 1;
            }
        } catch ( IOException ioe ) {
            System.out.println("written " + written);
        }
        assert(testRecovery() == (written * DATA_SIZE));
    }   
    
     
    @Test
    public void testMultiSegmentException() throws Exception {
        long written = 0;
        try {
            while ( true ) {
                manager.write(new WrappingChunk(ByteBuffer.allocate(DATA_SIZE)), current++);
                if ( written++ > 1024 ) {
                    manager.setBufferBuilder(new ExceptionBuilder(512 * 1024));
                }
            }
        } catch ( IOException ioe ) {
            System.out.println("written " + written);
        }
        assert(testRecovery() == (written * DATA_SIZE));
    }  
    
    @Test
    public void testMiddleSegmentException() throws Exception {
        manager.setBufferBuilder(new ExceptionBuilder((512 * 1024) + 916));
        long written = 0;
        try {
            while ( true ) {
                manager.write(new WrappingChunk(ByteBuffer.allocate(DATA_SIZE)), current++);
                written += 1;
            }
        } catch ( IOException ioe ) {
            System.out.println("written " + written);
        }
        assert(testRecovery() == (written * DATA_SIZE));
    }      
    
     
    @Test
    public void testSyncWritesException() throws Exception {
        manager.setBufferBuilder(new ExceptionBuilder(512 * 1024 + 916));
        long written = 0;
        try {
            while ( true ) {
                manager.write(new WrappingChunk(ByteBuffer.allocate(DATA_SIZE)), current++);
                manager.sync();
                written += 1;
            }
        } catch ( IOException ioe ) {
            System.out.println("written " + written);
        }
        assert(testRecovery() == (written * DATA_SIZE));
    }    
        
     
    @Test
    public void testCountUpSegment() throws Exception {
        long written = 0;
        for (int x=0;x<1*1024*1024;x+=2000) {
            manager.setBufferBuilder(new ExceptionBuilder(x));
            try {
                while ( true ) {
                    manager.write(new WrappingChunk(ByteBuffer.allocate(DATA_SIZE)), current++);
                    manager.sync();
                    written += 1;
                }
            } catch ( IOException ioe ) {
                System.out.println("written " + written);
            }
            assert(testRecovery() == (written * DATA_SIZE));
        }
    }    
        
    public long testRecovery() throws Exception {
        long size = 0;
        manager.close();
        manager = new NIOManager(Configuration.getConfiguration(workArea),src);
        System.out.println(manager.getStatistics());

        manager.seek(IOManager.Seek.END.getValue());
        Chunk c = manager.read(Direction.REVERSE);
        int count = 0;
        while ( c != null ) {
            count++;
            size += c.length();
            c = manager.read(Direction.REVERSE);
        }
        System.out.println(manager.getMinimumMarker());
        System.out.println(manager.getCurrentMarker());
        System.out.println("chunks after clean " + count);
        return size;
    }
    
    @After
    public void tearDown() throws Exception {
        manager.close();
        manager = null;
        System.gc();
    }
}
