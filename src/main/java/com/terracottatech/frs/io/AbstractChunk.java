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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import static com.terracottatech.frs.util.ByteBufferUtils.BYTE_SIZE;
import static com.terracottatech.frs.util.ByteBufferUtils.SHORT_SIZE;
import static com.terracottatech.frs.util.ByteBufferUtils.INT_SIZE;
import static com.terracottatech.frs.util.ByteBufferUtils.LONG_SIZE;


/**
 * @author mscott
 */
public abstract class AbstractChunk implements Chunk {


    private static class BufferReference {
        private final ByteBuffer current;
        private final int        position;
        BufferReference(ByteBuffer buf, int pos) {
            current = buf;
            position = pos;
        }

        public byte get() {
            return current.get(position);
        }
        public short getShort() {
            return current.getShort(position);
        }
        public int getInt() {
            return current.getInt(position);
        }
        public long getLong() {
            return current.getLong(position);
        }
    }


    @Override
    public ByteBuffer[] getBuffers(long length) {
        ByteBuffer[] list = getBuffers();
        ArrayList<ByteBuffer> copy = new ArrayList<ByteBuffer>();
        long count = 0;

        if ( length == 0 ) return new ByteBuffer[0];

        for (ByteBuffer buffer : list ) {
            if ( !buffer.hasRemaining() ) {
                continue;
            }
            if ( buffer.remaining() >= length - count ) {
                int restore = buffer.limit();
                buffer.limit(buffer.position() + (int)(length-count));
                copy.add(buffer.slice());
                buffer.position(buffer.limit()).limit(restore);
                count = length;
            } else {
                copy.add(buffer.duplicate());
                count += buffer.remaining();
                buffer.position(buffer.limit());
            }
            if ( count == length ) {
                break;
            }
        }
        return copy.toArray(new ByteBuffer[copy.size()]);
    }


    @Override
    public Chunk getChunk(long length) {
        final ByteBuffer[] list = getBuffers(length);
        return new AbstractChunk() {

            @Override
            public ByteBuffer[] getBuffers() {
                return list;
            }
        };
    }

    private BufferReference scanTo(long position) {
        ByteBuffer[] list = getBuffers();
        long seek = 0;
        for (ByteBuffer buffer : list ) {
            if ( seek + buffer.limit() > position ) {
                return new BufferReference(buffer,(int)(position-seek));
            }
            seek += buffer.limit();
        }
        throw new IndexOutOfBoundsException("scanTo: position=" + position + " buffers.length=" + list.length);
    }

    private ByteBuffer findEndForPut(int size) {
        return getBuffers()[findEndForPut(size, 0)];
    }

    private int findEndForPut(int size, int from) {
        ByteBuffer[] list = getBuffers();
        for (; from < list.length; from++) {
            if (list[from].isReadOnly() || !list[from].hasRemaining()) {
                continue;
            } else if (list[from].remaining() < size) {
                list[from].limit(list[from].position());
                continue;
            }
            return from;
        }
        throw new IndexOutOfBoundsException("findEndForPut: size=" + size + " from=" + from + " buffers.length=" + list.length);
    }

    private ByteBuffer findEndForGet(int size) {
        return getBuffers()[findEndForGet(size, 0)];
    }

    private int findEndForGet(int size, int from) {
        ByteBuffer[] list = getBuffers();
        for (; from < list.length; from++) {
            if (!list[from].hasRemaining()) {
                continue;
            } else if (list[from].remaining() < size) {
                throw new BufferUnderflowException();
            }
            return from;
        }
        throw new IndexOutOfBoundsException("findEndForGet: size=" + size + " from=" + from + " buffers.length=" + list.length);
    }

    @Override
    public byte get(long pos) {
        return scanTo(pos).get();
    }

     @Override
    public short getShort(long pos) {
        return scanTo(pos).getShort();
    }

    @Override
    public int getInt(long pos) {
        return scanTo(pos).getInt();
    }

    @Override
    public long getLong(long pos) {
        return scanTo(pos).getLong();
    }

    @Override
    public byte get() {
        return findEndForGet(BYTE_SIZE).get();
    }

    @Override
    public short getShort() {
        return findEndForGet(SHORT_SIZE).getShort();
    }

    @Override
    public int getInt() {
        return findEndForGet(INT_SIZE).getInt();
    }

    @Override
    public long getLong() {
        return findEndForGet(LONG_SIZE).getLong();
    }

    @Override
    public byte peek() {
        ByteBuffer target = findEndForGet(BYTE_SIZE);
        return target.get(target.position());
    }

    @Override
    public short peekShort() {
        ByteBuffer target = findEndForGet(SHORT_SIZE);
        return target.getShort(target.position());
    }

    @Override
    public int peekInt() {
        ByteBuffer target = findEndForGet(INT_SIZE);
        return target.getInt(target.position());
    }

    @Override
    public long peekLong() {
        ByteBuffer target = findEndForGet(LONG_SIZE);
        return target.getLong(target.position());
    }

    @Override
    public void put(byte v) {
        findEndForPut(BYTE_SIZE).put(v);
    }

    @Override
    public void putShort(short v) {
        findEndForPut(SHORT_SIZE).putShort(v);
    }

    @Override
    public void putInt(int v) {
        findEndForPut(INT_SIZE).putInt(v);
    }

    @Override
    public void putLong(long v) {
        findEndForPut(LONG_SIZE).putLong(v);
    }

    @Override
    public int get(byte[] buf) {
        int count = 0, from = 0;
        while (count < buf.length && this.hasRemaining()) {
            from = findEndForGet(BYTE_SIZE, from);
            ByteBuffer target = getBuffers()[from];
            int pos = target.position();
            int sw = buf.length-count;
            count += target.get(buf,count,(sw > target.remaining()) ? target.remaining() : sw).position() - pos;
        }
        return count;
    }

    @Override
    public int put(byte[] buf) {
        int count = 0, from = 0;
        while (count < buf.length) {
            from = findEndForPut(BYTE_SIZE, from);
            ByteBuffer target = getBuffers()[from];
            int pos = target.position();
            int sw = buf.length-count;
            count += target.put(buf,count,(sw > target.remaining()) ? target.remaining() : sw).position() - pos;
        }
        return count;
    }

    @Override
    public void skip(long jump) {
        if (jump == 0) {
            return;
        }
        long count = 0;
        int from = 0;
        while (count < jump) {
            from = findEndForGet(SHORT_SIZE, from);
            ByteBuffer target = getBuffers()[from];
            if (jump - count > target.remaining()) {
                count += target.remaining();
                target.position(target.limit());
            } else {
                target.position(target.position()+(int)(jump-count));
                return;
            }
        }
        throw new IndexOutOfBoundsException("skip: jump=" + jump + " buffers.length=" + getBuffers().length);
    }

    @Override
    public void flip() {
        ByteBuffer[] list = getBuffers();
        for (ByteBuffer buf : list ) {
            buf.flip();
        }
    }

    @Override
    public void clear() {
        ByteBuffer[] list = getBuffers();
        for (ByteBuffer buf : list ) {
            buf.clear();
        }
    }


    @Override
    public long length()  {
        long len = 0;
        for ( ByteBuffer buf : getBuffers() ) {
            len += buf.limit();
        }
        return len;
    }

    @Override
    public void limit(long v) {
        long len = 0;
        for ( ByteBuffer buf : getBuffers() ) {
            len += buf.limit();
            if ( len > v ) {
                buf.limit(buf.limit()-(int)(len-v));
                return;
            }
        }
    }

    @Override
    public long remaining()  {
        long len = 0;
        for ( ByteBuffer buf : getBuffers() ) {
            len += buf.remaining();
        }
        return len;
    }

    @Override
    public long position() {
        long position = 0;
        for ( ByteBuffer buf : getBuffers() ) {
            position += buf.position();
            if ( buf.hasRemaining() ) {
                break;
            }
        }
        return position;
    }

    @Override
    public boolean hasRemaining()  {
        for ( ByteBuffer buf : getBuffers() ) {
            if ( buf.hasRemaining() ) return true;
        }
        return false;
    }

}