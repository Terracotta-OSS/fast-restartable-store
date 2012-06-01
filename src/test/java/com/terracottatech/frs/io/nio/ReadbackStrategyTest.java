/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.FileBuffer;
import com.terracottatech.frs.io.WrappingChunk;
import com.terracottatech.frs.util.ByteBufferUtils;
import com.terracottatech.frs.util.JUnitTestFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
/**
 *
 * @author mscott
 */
public class ReadbackStrategyTest {
    
    FileBuffer  buffer;
    File        src;
    
    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder();    
    
    public ReadbackStrategyTest() {
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
        src = folder.newFile();
        FileChannel fc = new FileOutputStream(src).getChannel();
        ByteBuffer buf = ByteBuffer.allocate(1024);
        buffer = new FileBuffer(fc, ByteBuffer.allocate(8192));
        WrappingChunk c = new WrappingChunk(buf);
        for (int x=0;x<1024;x++) {
            append(c,0);
            c.clear();
        }

   }

    //  from nio segment code  
    public void append(Chunk c, long max) throws Exception {
        buffer.clear();
        buffer.partition(ByteBufferUtils.LONG_SIZE + ByteBufferUtils.INT_SIZE);
        long amt = c.remaining();
        assert (amt == c.length());
        buffer.put(SegmentHeaders.CHUNK_START.getBytes());
        buffer.putLong(amt);
        buffer.write(1);
        buffer.writeDirect(c.getBuffers());
//        buffer.insert(c.getBuffers(), 1);
        buffer.clear();
        buffer.putLong(amt);
        buffer.putLong(max);
        buffer.put(SegmentHeaders.FILE_CHUNK.getBytes());
        try {
            buffer.write(1);
        } finally {

        }    
    }
    
    @Test
    public void testDeleteMappedFile() throws Exception {
        File f = folder.newFile();
        FileOutputStream fos = new FileOutputStream(f);
        for (int x=0;x<1024;x++) fos.write(0x00AB);
        fos.close();
        FileInputStream fis = new FileInputStream(f);
        
        MappedByteBuffer buffer = fis.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, f.length());
        for (int x=0;x<1024;x++) System.out.println(buffer.get(x));
        f.delete();
        assert(!f.exists());
        for (int x=0;x<1024;x++) assert((buffer.get(x) & 0xff) == 0xAB);
    }
    
    @Test
    public void testIntegrityReadback() throws Exception {
 //  testing normal close
        buffer.clear();
        buffer.put(SegmentHeaders.CLOSE_FILE.getBytes());
        buffer.write(1);
        buffer.close();
        
        buffer = new FileBuffer(new FileInputStream(src).getChannel(),ByteBuffer.allocateDirect(8192));
        IntegrityReadbackStrategy i = new IntegrityReadbackStrategy(buffer);
        int count = 0;
        while ( i.hasMore(Direction.FORWARD) ) {
            count++;
            i.iterate(Direction.FORWARD);
        }
        assertThat(count,is(1024));
        byte[] check = new byte[4];
        buffer.partition(4);
        buffer.read(1);
        buffer.get(check);
        System.out.println(new String(check));
        assert(SegmentHeaders.CLOSE_FILE.validate(check));
    }
    
    
    @Test @Ignore
    public void testIntegrityReadbackOnAbort() throws Exception {
//  testing abort close
        buffer.sync();
        
        final AtomicInteger count = new AtomicInteger();
        final ByteBuffer[] list = new ByteBuffer[8192];
        for (int x=0;x<list.length;x++) {
            list[x] = ByteBuffer.allocate(4);
        }
//  cause really slow writes in the hopes of catching the thread mid-write
        Thread t = new Thread() {
            public void run() {
                WrappingChunk c = new WrappingChunk(list);
                while (true) {
                    try {
                        append(c,0);
                        count.incrementAndGet();
                        c.clear();
                    } catch ( Exception e ) {
                        e.printStackTrace();
                        break;
                    }
                }
            }
        };
        
        t.start();
        Thread.sleep((int)Math.round(Math.random()*1000));
        t.interrupt();
        buffer.close();
                        
        
        buffer = new FileBuffer(new FileInputStream(src).getChannel(),ByteBuffer.allocate(8192));
        IntegrityReadbackStrategy i = new IntegrityReadbackStrategy(buffer);
        int check = 0;
        while ( i.hasMore(Direction.FORWARD) ) {
            check++;
            i.iterate(Direction.FORWARD);
        }
        assert(check > 1024);
//        byte[] check = new byte[4];
//        buffer.partition(4);
//        buffer.read(1);
//        buffer.get(check);
//        System.out.println(new String(check));
//        assert(SegmentHeaders.CLOSE_FILE.validate(check));
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
