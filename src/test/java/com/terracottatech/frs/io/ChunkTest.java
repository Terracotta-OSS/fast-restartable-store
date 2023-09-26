/*
 * Copyright (c) 2012-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
import org.junit.*;

/**
 *
 * @author mscott
 */
public class ChunkTest extends RandomGenerator {

    Chunk[] chunkTypes = new Chunk[]{
        new ProgressiveChunkImpl(),
        new RevProgressiveChunkImpl(),
        new RandomChunkImpl()
    };

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
        for (Chunk c : chunkTypes) {
        long tl = (long) (Math.random() * Long.MAX_VALUE);
        int ti = (int) (Math.random() * Integer.MAX_VALUE);
        short ts = (short) (Math.random() * Short.MAX_VALUE);
        String tb = ("this is a test string");

        c.putLong(tl);
        c.putShort(ts);
        c.putInt(ti);
        c.put(tb.getBytes());

        for (ByteBuffer b : c.getBuffers()) {
            b.flip();
        }

        assert (tl == c.getLong());
        assert (ts == c.getShort());
        assert (ti == c.getInt());
        byte[] size = new byte[tb.length()];
        c.get(size);
        System.out.println(new String(size));
        assert (tb.equals(new String(size)));
        }
    }

    @Test
    public void testLength() {
        for (Chunk c : chunkTypes) {
        long length = c.length();
        long t = 0;
        for (ByteBuffer b : c.getBuffers()) {
            t += b.limit();
        }
        assert (length == t);
        byte[] test = new byte[lessThan((int) length)];
        for (int x = 0; x < test.length; x++) {
            test[x] = (byte) lessThan(256);
        }
        c.put(test);
        length = c.remaining();
        assert (length == t - test.length);
        }
    }

    @Test
    public void testRemaining() {
        for (Chunk c : chunkTypes) {
        long length = c.length();

        byte[] test = new byte[lessThan((int) length)];
        for (int x = 0; x < test.length; x++) {
            test[x] = (byte) lessThan(256);
        }
        c.put(test);
        assert (c.length() == c.remaining() + test.length);
        }
    }

    @Test
    public void testByte() {
        for (Chunk c : chunkTypes) {
            byte r = lessThan(Byte.MAX_VALUE);
            c.put(r);
            c.flip();
            byte b = c.peek();
            assert (b == r);
            b = c.get();
            assert (b == r);
            assert (!c.getBuffers()[0].hasRemaining());
            assert (c.get(0) == r);
        }
    }

    @Test
    public void testShort() {
        for (Chunk c : chunkTypes) {
            short r = lessThan(Short.MAX_VALUE);
            c.putShort(r);
            c.flip();
            short b = c.peekShort();
            assert (b == r);
            b = c.getShort();
            assert (b == r);
            assert (!c.getBuffers()[0].hasRemaining());
            assert (!c.getBuffers()[1].hasRemaining());
            assert (c.getShort(0) == r);
        }
    }

    @Test
    public void testInt() {
        for (Chunk c : chunkTypes) {
            int r = lessThan(Integer.MAX_VALUE);
            c.putInt(r);
            c.flip();
            int b = c.peekInt();
            assert (b == r);
            b = c.getInt();
            assert (b == r);
            assert (!c.getBuffers()[0].hasRemaining());
            assert (!c.getBuffers()[1].hasRemaining());
            assert (!c.getBuffers()[2].hasRemaining());
            assert (c.getInt(0) == r);
        }
    }

    @Test
    public void testLong() {
        for (Chunk c : chunkTypes) {
            long r = lessThan(Long.MAX_VALUE);
            c.putLong(r);
            c.flip();
            long b = c.peekLong();
            assert (b == r);
            b = c.getLong();
            assert (b == r);
            assert (!c.getBuffers()[0].hasRemaining());
            assert (!c.getBuffers()[1].hasRemaining());
            assert (!c.getBuffers()[2].hasRemaining());
            assert (!c.getBuffers()[3].hasRemaining());
            assert (c.getLong(0) == r);
        }
    }

    @Test
    public void testFill() {
        for (Chunk c : chunkTypes) {
            int size = (int) c.length();
            byte[] fill = new byte[size];
            for (int x = 0; x < fill.length; x++) {
                fill[x] = lessThan(Byte.MAX_VALUE);
            }
            c.put(fill);
            c.flip();
            for (int x = 0; x < fill.length; x++) {
                assert (c.get(x) == fill[x]);
            }
        }
    }

    class RevProgressiveChunkImpl extends AbstractChunk {

        ByteBuffer[] base = new ByteBuffer[]{
            ByteBuffer.allocate(64),
            ByteBuffer.allocate(32),
            ByteBuffer.allocate(16),
            ByteBuffer.allocate(8),
            ByteBuffer.allocate(4),
            ByteBuffer.allocate(2),
            ByteBuffer.allocate(1)
        };

        public RevProgressiveChunkImpl() {
        }

        @Override
        public ByteBuffer[] getBuffers() {
            return base;
        }
    }

    class ProgressiveChunkImpl extends AbstractChunk {

        ByteBuffer[] base = new ByteBuffer[]{
            ByteBuffer.allocate(1),
            ByteBuffer.allocate(2),
            ByteBuffer.allocate(4),
            ByteBuffer.allocate(8),
            ByteBuffer.allocate(16),
            ByteBuffer.allocate(32),
            ByteBuffer.allocate(64),
            ByteBuffer.allocate(128)};

        public ProgressiveChunkImpl() {
        }

        @Override
        public ByteBuffer[] getBuffers() {
            return base;
        }
    }

    class RandomChunkImpl extends AbstractChunk {

        ByteBuffer[] base;

        public RandomChunkImpl() {
            base = new ByteBuffer[8];
            for (int x = 0; x < base.length; x++) {
                base[x] = ByteBuffer.allocate(lessThan(64));
            }

        }

        @Override
        public ByteBuffer[] getBuffers() {
            return base;
        }
    }
}
