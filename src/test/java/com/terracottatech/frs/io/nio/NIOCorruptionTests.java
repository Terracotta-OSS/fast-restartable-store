/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.config.Configuration;
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
public class NIOCorruptionTests {
    File workArea;
    BufferBuilder  builder;
    NIOManager manager;
    long current;
    long min;
    
    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder();        
    
    public NIOCorruptionTests() {
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
        Properties props = new Properties();
        props.setProperty("io.nio.segmentSize", Integer.toString(1 * 1024 * 1024));
        props.setProperty("io.nio.memorySize", Integer.toString(10 * 1024 * 1024));
//        props.setProperty("io.nio.bufferBuilder", "com.terracottatech.frs.io.nio.CorruptionBuilder");
        props.store(new FileWriter("frs.properties"), "frs test properties");
        
        Configuration config = Configuration.getConfiguration(workArea);
        manager = new NIOManager(config);
        manager.setMinimumMarker(100);
        current = 100;
    }
     
    @Test
    public void testOneSegmentException() throws Exception {
        manager.setBufferBuilder(new ExceptionBuilder(300 * 1024));
        long written = 0;
        try {
            while ( true ) {
                manager.write(new WrappingChunk(ByteBuffer.allocate(10 *1024)), current++);
                written += 1;
            }
        } catch ( IOException ioe ) {
            manager.sync();
            System.out.println(manager.getStatistics());
        }
        assert(testRecovery() == (written * 10 * 1024));
    }   
    
     
    @Test
    public void testTenSegmentException() throws Exception {
        manager.setBufferBuilder(new ExceptionBuilder(10 * 1024 * 1024));
        long written = 0;
        try {
            while ( true ) {
                manager.write(new WrappingChunk(ByteBuffer.allocate(10 *1024)), current++);
                written += 1;
            }
        } catch ( IOException ioe ) {
            manager.sync();
            System.out.println(manager.getStatistics());
        }
        assert(testRecovery() == (written * 10 * 1024));
    }  
    
    @Test
    public void testMiddleSegmentException() throws Exception {
        manager.setBufferBuilder(new ExceptionBuilder(10 * 1024 * 1024 + 916));
        long written = 0;
        try {
            while ( true ) {
                manager.write(new WrappingChunk(ByteBuffer.allocate(10 *1024)), current++);
                written += 1;
            }
        } catch ( IOException ioe ) {
            manager.sync();
            System.out.println(manager.getStatistics());
        }
        assert(testRecovery() == (written * 10 * 1024));
    }      
    
    public long testRecovery() throws Exception {
        long size = 0;
        manager.close();
        manager = new NIOManager(Configuration.getConfiguration(workArea));
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
    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}
}
