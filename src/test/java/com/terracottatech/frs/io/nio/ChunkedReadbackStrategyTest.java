/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.*;
import com.terracottatech.frs.util.JUnitTestFolder;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.junit.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
/**
 *
 * @author mscott
 */
public class ChunkedReadbackStrategyTest {
    
    NIOSegmentImpl  segment;
    NIOStreamImpl   stream;
    NIOSegmentList  list;
    
    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder();    
    
    public ChunkedReadbackStrategyTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() throws Exception {
        System.out.println(folder.getRoot());
        stream = new NIOStreamImpl(folder.getRoot(), 1024 * 1024);
        stream.setMinimumMarker(100);
        stream.setMaximumMarker(100);
        stream.setMarker(100);
        for (int x=0;x<1024;x++) {
            WrappingChunk c = new WrappingChunk(ByteBuffer.wrap(("test me please " + x).getBytes()));
            stream.append(c);
        }
        stream.close();
        list = new NIOSegmentList(folder.getRoot());
   }
    
    @Test
    public void testReadback() throws Exception {
 //  testing normal close
        list.setReadPosition(0);
        int count = 0;
        
        File f = list.nextReadFile(Direction.FORWARD);
        while ( f != null ) {
            FileChannel channel = new FileInputStream(f).getChannel();
            FileBuffer buffer = new FileBuffer(channel, ByteBuffer.allocateDirect(512 * 1024));
            ManualBufferSource src = new ManualBufferSource(100 * 1024 * 1024);
            ChunkedReadbackStrategy strat = new ChunkedReadbackStrategy(buffer, src);
            
            
            while ( strat.hasMore(Direction.FORWARD) ) {
                Chunk c = strat.iterate(Direction.FORWARD);                
                
                count += 1;
                if ( count > 1020 ) {
                    System.out.println("rotate");
                }
                byte[] get = new byte[(int)c.remaining()];
                c.get(get);
                System.out.println(new String(get));
            }
            channel.close();
            f = list.nextReadFile(Direction.FORWARD);
        }
        assertThat(count, is(1024));
    }
    
    
    @Test
    public void testReverseReadback() throws Exception {
 //  testing normal close
        list.setReadPosition(-1);
        int count = 0;
        
        File f = list.nextReadFile(Direction.REVERSE);
        while ( f != null ) {
            FileChannel channel = new FileInputStream(f).getChannel();
            FileBuffer buffer = new FileBuffer(channel, ByteBuffer.allocateDirect(512 * 1024));
            ManualBufferSource src = new ManualBufferSource(100 * 1024 * 1024);
            ChunkedReadbackStrategy strat = new ChunkedReadbackStrategy(buffer, src);
            
            
            while ( strat.hasMore(Direction.REVERSE) ) {
                Chunk c = strat.iterate(Direction.REVERSE);                
                
                count += 1;
                if ( count > 1020 ) {
                    System.out.println("rotate");
                }
                byte[] get = new byte[(int)c.remaining()];
                c.get(get);
                System.out.println(new String(get));
            }
            channel.close();
            f = list.nextReadFile(Direction.REVERSE);
        }
        assertThat(count, is(1024));
    }    
    
    @After
    public void tearDown() {
    }
    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}
}
