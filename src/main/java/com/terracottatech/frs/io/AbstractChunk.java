/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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
 *
 * @author mscott
 */
public abstract class AbstractChunk implements Chunk {
    
    
    private static class BufferReference {
        ByteBuffer current;
        int        position;
        BufferReference(ByteBuffer buf, int pos) {
            current = buf;
            position = pos;
        }
        
        public byte get() {
            return current.get(position);
        }
        public short getShort() {
            if ( current.limit() < SHORT_SIZE) throw new BufferUnderflowException();
            return current.getShort(position);
        }
        public int getInt() {
            if ( current.limit() < INT_SIZE) throw new BufferUnderflowException();
            return current.getInt(position);
        }
        public long getLong() {
            if ( current.limit() < LONG_SIZE) throw new BufferUnderflowException();
            return current.getLong(position);
        }
    }
    
    
    @Override
    public ByteBuffer[] getBuffers(long length) {
        ByteBuffer[] list = getBuffers();
        ArrayList<ByteBuffer> copy = new ArrayList<ByteBuffer>();
        long count = 0;
        
        if ( length == 0 ) return new ByteBuffer[0];
        
        for (int x=0;x<list.length;x++) {
            if ( !list[x].hasRemaining() ) {
                continue;
            }
            if ( list[x].remaining() >= length - count ) {
                ByteBuffer add = list[x].slice();
                add.limit((int)(length-count));
                copy.add(add);
                list[x].position(list[x].position() + (int)(length-count));
                count = length;
            } else {
                copy.add(list[x].duplicate());
                count += list[x].remaining();
                list[x].position(list[x].limit());
            }
            if ( count == length ) {
                break;
            }
        }
        return copy.toArray(new ByteBuffer[copy.size()]);
    }
    
    private BufferReference scanTo(long position) {
        ByteBuffer[] list = getBuffers();
        long seek = 0;
        for (int x=0;x<list.length;x++) {
            if ( seek + list[x].limit() > position ) {
                return new BufferReference(list[x],(int)(position-seek));
            }
            seek += list[x].limit();
        }
        throw new IndexOutOfBoundsException();
    }
    
    private ByteBuffer findEnd(int size,boolean forPut) {
        ByteBuffer[] list = getBuffers();
        for (int x=0;x<list.length;x++) {
            if ( forPut && list[x].isReadOnly() ) {
                continue;
            } else if ( !list[x].hasRemaining() ) {
                continue;
            } else if ( list[x].remaining() < size ) {
                if ( forPut ) list[x].limit(list[x].position());
                else throw new BufferUnderflowException();
                continue;
            }
            return list[x];
        }
        throw new IndexOutOfBoundsException();
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
        return findEnd(BYTE_SIZE,false).get();
    } 

    @Override
    public short getShort() {
        return findEnd(SHORT_SIZE,false).getShort();
    }    
    
    @Override
    public int getInt() {
        return findEnd(INT_SIZE,false).getInt();
    }
    
    @Override
    public long getLong() {
        return findEnd(LONG_SIZE,false).getLong();
    }  
     
    @Override
    public byte peek() {
        ByteBuffer target = findEnd(BYTE_SIZE,false);
        return target.get(target.position());
    } 

    @Override
    public short peekShort() {
        ByteBuffer target = findEnd(SHORT_SIZE,false);
        return target.getShort(target.position());
    }    
    
    @Override
    public int peekInt() {
        ByteBuffer target = findEnd(INT_SIZE,false);
        return target.getInt(target.position());
    }
    
    @Override
    public long peekLong() {
        ByteBuffer target = findEnd(LONG_SIZE,false);
        return target.getLong(target.position());
    }    
    
    @Override
    public void put(byte v) {
        findEnd(BYTE_SIZE,true).put(v);
    }
    
    @Override
    public void putShort(short v) {
        findEnd(SHORT_SIZE,true).putShort(v);
    }
    
    @Override
    public void putInt(int v) {
        findEnd(INT_SIZE,true).putInt(v);
    }
    
    @Override
    public void putLong(long v) {
        findEnd(LONG_SIZE,true).putLong(v);
    }

    @Override
    public int get(byte[] buf) {
        int count = 0;
        while ( count < buf.length ) {
            ByteBuffer target = findEnd(BYTE_SIZE,true);
            int pos = target.position();
            int sw = buf.length-count;
            count += target.get(buf,count,(sw > target.remaining()) ? target.remaining() : sw).position() - pos;            
        }
        return count;
    }

    @Override
    public int put(byte[] buf) {
        int count = 0;
        while ( count < buf.length ) {
            ByteBuffer target = findEnd(BYTE_SIZE,true);
            int pos = target.position();
            int sw = buf.length-count;
            count += target.put(buf,count,(sw > target.remaining()) ? target.remaining() : sw).position() - pos;            
        }
        return count;
    }
    
    @Override
    public void skip(long jump) {
        long count = 0;
        while ( count < jump ) {
            ByteBuffer target = findEnd(SHORT_SIZE,true);
            if ( jump - count > target.remaining() ) {
                count += target.remaining();
                target.position(target.limit());
            } else {
                target.position(target.position()+(int)(jump-count));
                return;
            }
        }
        throw new IndexOutOfBoundsException();
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
    public boolean hasRemaining()  {
        for ( ByteBuffer buf : getBuffers() ) {
            if ( buf.hasRemaining() ) return true;
        }
        return false;
    }   
    
}
