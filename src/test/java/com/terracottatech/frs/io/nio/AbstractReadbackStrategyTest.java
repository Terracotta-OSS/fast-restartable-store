/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.FileBuffer;
import com.terracottatech.frs.io.WrappingChunk;
import com.terracottatech.frs.util.JUnitTestFolder;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;
import junit.framework.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Rule;

/**
 *
 * @author mscott
 */
public abstract class AbstractReadbackStrategyTest {
    Random r = new Random();
    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder(); 
    
    public AbstractReadbackStrategyTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }
    
    public abstract ReadbackStrategy getReadbackStrategy(Direction dir, FileBuffer buffer) throws IOException;
    /**
     * Test of getMaximumMarker method, of class BufferedRandomAccessStrategy.
     */
    @Test
    public void testGetMaximumMarker() throws Exception {
        System.out.println("getMaximumMarker");
        FileBuffer buffer = new FileBuffer(new RandomAccessFile(folder.newFile(),"rw").getChannel(),ByteBuffer.allocate(8192));
        String phrase = "random access reads from the log stream";
        int count = writeCompleteBuffer(buffer, phrase);
        ReadbackStrategy instance = getReadbackStrategy(Direction.RANDOM, buffer);
        long expResult = count - 1;
        long result = instance.getMaximumMarker();
        assertEquals(expResult, result);
    }

    /**
     * Test of iterate method, of class BufferedRandomAccessStrategy.
     */
    @Test
    public void testIterate() throws Exception {
        File target = folder.newFile();
        FileBuffer buffer = new FileBuffer(new RandomAccessFile(target,"rw").getChannel(),ByteBuffer.allocate(8192));
        String phrase = "this test will iterate forward and backward";
        String[] list = phrase.split(" ");
        long count = 100;
        List<Long> jumps = new ArrayList<Long>();
        for ( String word : list ) {
            this.writeDataToChunk(word.getBytes(), buffer, count++);
            buffer.write(1);
            jumps.add(buffer.offset());
            buffer.clear();
        }
        this.writeJumplistToChunk(jumps, buffer);
        buffer.write(1);
        buffer.position(0);
        
        buffer.close();
        buffer = new FileBuffer(new RandomAccessFile(target,"rw").getChannel(),ByteBuffer.allocate(8192));
        ReadbackStrategy instance = getReadbackStrategy(Direction.FORWARD, buffer);
        assertTrue(instance.hasMore(Direction.FORWARD));
        for ( String word : list ) {
            Chunk c = instance.iterate(Direction.FORWARD);
            assertTrue(c.remaining() == word.length());
            byte[] check = new byte[word.length()];
            c.get(check);
            assertEquals(word, new String(check));
        }
        assertFalse(instance.hasMore(Direction.FORWARD));
        
        buffer.close();
        buffer = new FileBuffer(new RandomAccessFile(target,"rw").getChannel(),ByteBuffer.allocate(8192));
        instance = getReadbackStrategy(Direction.REVERSE, buffer);
        assertTrue(instance.hasMore(Direction.REVERSE));
        for ( String word : reverse(list) ) {
            Chunk c = instance.iterate(Direction.REVERSE);
            assertTrue(c.remaining() == word.length());
            byte[] check = new byte[word.length()];
            c.get(check);
            assertEquals(word, new String(check));
        }
        assertFalse(instance.hasMore(Direction.REVERSE));
        
        buffer.close();
        buffer = new FileBuffer(new RandomAccessFile(target,"rw").getChannel(),ByteBuffer.allocate(8192));
        instance = getReadbackStrategy(Direction.FORWARD, buffer);
        assertTrue(instance.hasMore(Direction.FORWARD));
        for ( String word : list ) {
            Chunk c = instance.iterate(Direction.FORWARD);
            assertTrue(c.remaining() == word.length());
            byte[] check = new byte[word.length()];
            c.get(check);
            assertEquals(word, new String(check));
        }
        assertFalse(instance.hasMore(Direction.FORWARD));
        assertTrue(instance.hasMore(Direction.REVERSE));

  //  then back
        for ( String word : reverse(list) ) {
            System.out.println(word);
            Chunk c = instance.iterate(Direction.REVERSE);
            assertTrue("length:" + c.remaining(), c.remaining() == word.length());
            byte[] check = new byte[word.length()];
            c.get(check);
            assertEquals(word, new String(check));
        }
        assertFalse(instance.hasMore(Direction.REVERSE));
    }
    
    private String[] reverse(String[] list) {
        String[] copy = Arrays.copyOf(list, list.length);
        for (int x=0;x<copy.length/2;x++) {
            copy[x] = list[list.length-x-1];
            copy[copy.length - x - 1] = list[x];
        }
        return copy;
    }

    /**
     * Test of hasMore method, of class BufferedRandomAccessStrategy.
     */
    @Test
    public void testHasMore() throws Exception {
//  test empty file
        File target = folder.newFile();
        System.out.println("hasMore");
        FileBuffer buffer = new FileBuffer(new RandomAccessFile(target,"rw").getChannel(),ByteBuffer.allocate(8192));
        
        buffer.close();
        buffer = new FileBuffer(new RandomAccessFile(target,"rw").getChannel(),ByteBuffer.allocate(8192));
        ReadbackStrategy instance = getReadbackStrategy(Direction.FORWARD, buffer);
        assertFalse(instance.hasMore(Direction.FORWARD));
    }

    /**
     * Test of isConsistent method, of class BufferedRandomAccessStrategy.
     */
    @Test
    public void testIsConsistent() throws Exception {
        FileBuffer buffer = new FileBuffer(new RandomAccessFile(folder.newFile(),"rw").getChannel(),ByteBuffer.allocate(8192));
        writeCompleteBuffer(buffer, "this is a consistent buffer");
        ReadbackStrategy instance = getReadbackStrategy(Direction.RANDOM, buffer);
        assertTrue(instance.isConsistent());
        
        buffer = new FileBuffer(new RandomAccessFile(folder.newFile(),"rw").getChannel(),ByteBuffer.allocate(8192));
        writePartialBuffer(buffer, "this is a partial buffer");
       instance = getReadbackStrategy(Direction.RANDOM, buffer);
        assertFalse(instance.isConsistent());
    }

    /**
     * Test of scan method, of class BufferedRandomAccessStrategy.
     */
    @Test
    public void testScan() throws Exception {
        FileBuffer buffer = new FileBuffer(new RandomAccessFile(folder.newFile(),"rw").getChannel(),ByteBuffer.allocate(8192));
        writeCompleteBuffer(buffer, "test scanning a complete buffer");
        ReadbackStrategy instance = getReadbackStrategy(Direction.RANDOM, buffer);
        //  starts at 100 so 103 should be "complete"
        Chunk c = instance.scan(103L);
        assertTrue(c.remaining() == "complete".length());
        byte[] data = new byte["complete".length()];
        c.get(data);
        assertEquals("complete",new String(data));
        
        buffer = new FileBuffer(new RandomAccessFile(folder.newFile(),"rw").getChannel(),ByteBuffer.allocate(8192));
        writePartialBuffer(buffer, "now scan a partial buffer");
       instance = new BufferedReadbackStrategy(Direction.RANDOM, buffer.getFileChannel(), null);
        c = instance.scan(103L);
        assertTrue(c.remaining() == "partial".length());
        data = new byte["partial".length()];
        c.get(data);
        assertEquals("partial",new String(data));
    }

    /**
     * Test of size method, of class BufferedRandomAccessStrategy.
     */
    @Test
    public void testSize() throws Exception {
        FileBuffer buffer = new FileBuffer(new RandomAccessFile(folder.newFile(),"rw").getChannel(),ByteBuffer.allocate(8192));
        writeCompleteBuffer(buffer, "test size by making sure buffer.size() gets called");
        long expected = buffer.size();
        ReadbackStrategy instance = getReadbackStrategy(Direction.RANDOM, buffer);
        //  starts at 100 so 104 should be "complete"
        System.out.println(instance.size());
        assertEquals(expected,instance.size());
    }
    
    /**
     * Test of isConsistent method, of class AbstractReadbackStrategy.
     */
    @Test
    public void testIsConsistent2() throws Exception {
        System.out.println("isConsistent");
        ByteBuffer buffer = null;
        AbstractReadbackStrategy instance = new AbstractReadbackStrategyImpl();
        assertFalse(instance.isConsistent());

        List<Long> expResult = randomizeJumpList(64);
        buffer = createCompleteJumpListBuffer(expResult);
        instance.readJumpList(buffer);
        assertTrue(instance.isConsistent());
    }

    /**
     * Test of readChunk method, of class AbstractReadbackStrategy.
     */
    @Test
    public void testReadChunk() throws Exception {
        System.out.println("readChunk");
        String phrase = "the cat jumped over the moon and skittles rained";
        AbstractReadbackStrategy instance = new AbstractReadbackStrategyImpl();
        ByteBuffer buffer = writePhrase(phrase);
        StringTokenizer tokens = new StringTokenizer(phrase," ");
        while ( tokens.hasMoreTokens() ) {
            Chunk c = new WrappingChunk(instance.readChunk(new WrappingChunk(buffer)));
            byte[] word = new byte[(int)c.length()];
            c.get(word);
            assertEquals(tokens.nextToken(),  new String(word));
        }
    }

    /**
     * Test of readJumpList method, of class AbstractReadbackStrategy.
     */
    @Test
    public void testReadJumpList() throws Exception {
        System.out.println("readJumpList");
        ByteBuffer buffer = null;
        AbstractReadbackStrategy instance = new AbstractReadbackStrategyImpl();
        List<Long> expResult = randomizeJumpList(64);
        buffer = createCompleteJumpListBuffer(expResult);
        List<Long> result = instance.readJumpList(buffer);
        assertEquals(expResult, result);
// test more than short length array
        expResult = randomizeJumpList(Short.MAX_VALUE + 8);
        buffer = createCompleteJumpListBuffer(expResult);
        result = instance.readJumpList(buffer);
        Assert.assertEquals(expResult.size(), result.size());
    }
    
    protected List<Long> randomizeJumpList(int size) {
        List<Long> list= new ArrayList<Long>();
        Random r = new Random();
        long base = 0;
        for (int x=0;x<size;x++) {
            base += r.nextInt(8192);
            list.add(base);
        }
        return list;
    }
    
    protected ByteBuffer createCompleteJumpListBuffer(List<Long> jumps) {
        ByteBuffer buffer = ByteBuffer.allocate(8 * jumps.size() + 10);  // size is a long(8) * size of array + 4 header + + 2 size + 4 footer
        writeJumplistToChunk(jumps, new WrappingChunk(buffer));
        buffer.flip();
        return buffer;
    }
    
    protected void writeJumplistToChunk(List<Long> jumps, Chunk target) {
        target.put(SegmentHeaders.CLOSE_FILE.getBytes());
        for (long jump : jumps) {
            target.putInt((int)jump);
        }
        if (jumps.size() < Integer.MAX_VALUE) {
            target.putInt((int) jumps.size());
        } else {
            target.putInt((int) -1);
        }
        target.put(SegmentHeaders.JUMP_LIST.getBytes());
    }
    
    protected ByteBuffer writePhrase(String phrase) {
        String[] words = phrase.split(" ");
        ByteBuffer buffer = ByteBuffer.allocate(0);
        long count = 0;
        for ( String word : words ) {
            buffer = writeChunk(word.getBytes(),buffer,count++);
        }
        buffer.flip();
        return buffer;
    }
    
    protected ByteBuffer writeChunk(byte[] data, ByteBuffer buffer, long maxMarker) {
        if ( buffer == null || buffer.remaining() < data.length + 8 + 8 + 4 + 8 + 4 ) {
            ByteBuffer transfer = ByteBuffer.allocate(buffer.capacity() + data.length * 2 + 64);
            buffer.flip();
            transfer.put(buffer);
            buffer = transfer;
        }
        writeDataToChunk(data,new WrappingChunk(buffer),maxMarker);
        return buffer;
    }
    
    protected void writeDataToChunk(byte[] data, Chunk base, long max) {
        base.put(SegmentHeaders.CHUNK_START.getBytes());
        base.putLong(data.length);
        base.put(data);
        base.putLong(data.length);
        base.putLong(max);
        base.put(SegmentHeaders.FILE_CHUNK.getBytes());
    }
    
    protected int writeCompleteBuffer(FileBuffer buffer, String phrase) throws Exception {
        String[] list = phrase.split(" ");
        long count = 100;
        List<Long> jumps = new ArrayList<Long>();
        for ( String word : list ) {
            this.writeDataToChunk(word.getBytes(), buffer, count++);
            buffer.write(1);
            jumps.add(buffer.offset());
            buffer.clear();
        }
        this.writeJumplistToChunk(jumps, buffer);
        buffer.write(1);
        buffer.position(0);
        return (int)count;
    }
    
    protected int writePartialBuffer(FileBuffer buffer, String phrase) throws Exception {
        String[] list = phrase.split(" ");
        long count = 100;
        List<Long> jumps = new ArrayList<Long>();
        for ( String word : list ) {
            this.writeDataToChunk(word.getBytes(), buffer, count++);
            buffer.write(1);
            jumps.add(buffer.offset());
            buffer.clear();
        }
        buffer.position(0);
        return (int)count;
    }       
    
    public class AbstractReadbackStrategyImpl extends AbstractReadbackStrategy {

        public AbstractReadbackStrategyImpl() {
        }

        @Override
        public boolean isConsistent() {
          return super.isCloseDetected();
        }

        @Override
        public Chunk iterate(Direction dir) throws IOException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public boolean hasMore(Direction dir) throws IOException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public long getMaximumMarker() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public long size() throws IOException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Chunk scan(long marker) throws IOException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
    }
}