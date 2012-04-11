/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
import org.junit.*;

/**
 *
 * @author mscott
 */
public class ChunkTest {
    
    public ChunkTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of getBuffers method, of class Chunk.
     */
    @Test
    public void testOps() {
        Chunk c = new ChunkImpl();
        long tl = (long)(Math.random() * Long.MAX_VALUE);
        int ti = (int)(Math.random() * Integer.MAX_VALUE);
        short ts = (short)(Math.random() * Short.MAX_VALUE);
        String tb = ("this is a test string");
        
        c.putLong(tl);
        c.putShort(ts);
        c.putInt(ti);
        c.put(tb.getBytes());
        
        for ( ByteBuffer b : c.getBuffers() ) {
            b.flip();
        }
        
        assert(tl == c.getLong());
        assert(ts == c.getShort());
        assert(ti == c.getInt());
        byte[] size = new byte[tb.length()];
        c.get(size);
        System.out.println(new String(size));
        assert(tb.equals(new String(size)));
    }
    
    class ChunkImpl extends AbstractChunk {
        
        ByteBuffer[]  base;

        public ChunkImpl() {
            base = new ByteBuffer[5];
            for (int x=0;x<base.length;x++) {
                base[x] = ByteBuffer.allocate(32);
            }
            
        }


        @Override
        public ByteBuffer[] getBuffers() {
            return base;
        }
        
    }
}
