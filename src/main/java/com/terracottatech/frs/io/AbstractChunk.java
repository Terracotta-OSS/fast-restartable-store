/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 *
 * @author mscott
 */
public abstract class AbstractChunk implements Chunk {
    
    
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

    @Override
    public long getLong() {
        ByteBuffer[] list = getBuffers();
        for (int x=0;x<list.length;x++) {
            if ( !list[x].hasRemaining() ) {
                continue;
            } else if ( list[x].remaining() < 8 ) {
                list[x].limit(list[x].position());
                continue;
            }
            
            return list[x].getLong();
        }
        throw new IndexOutOfBoundsException();
    }

    @Override
    public short getShort() {
        ByteBuffer[] list = getBuffers();
        for (int x=0;x<list.length;x++) {
            if ( !list[x].hasRemaining() ) {
                continue;
            } else if ( list[x].remaining() < 2 ) {
                list[x].limit(list[x].position());
                continue;
            }
            return list[x].getShort();
        }
        throw new IndexOutOfBoundsException();
    }
    
    @Override
    public int getInt() {
        ByteBuffer[] list = getBuffers();
        for (int x=0;x<list.length;x++) {
            if ( !list[x].hasRemaining() ) {
                continue;
            } else if ( list[x].remaining() < 4 ) {
                list[x].limit(list[x].position());
                continue;
            }
            return list[x].getInt();
        }
        throw new IndexOutOfBoundsException();
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
    public long remaining()  {
        long len = 0;
        for ( ByteBuffer buf : getBuffers() ) {
            len += buf.remaining();
        }
        return len;
    }
    
    
    @Override
    public boolean hasRemaining()  {
        long len = 0;
        for ( ByteBuffer buf : getBuffers() ) {
            if ( buf.hasRemaining() ) return true;
        }
        return false;
    }    

    @Override
    public void putLong(long v) {
        ByteBuffer[] list = getBuffers();
        for (int x=0;x<list.length;x++) {
            if ( !list[x].hasRemaining() ) {
                continue;
            } else if ( list[x].remaining() < 8 ) {
                list[x].limit(list[x].position());
            }
            list[x].putLong(v);
            break;
        }
    }

    @Override
    public void putShort(short v) {
        ByteBuffer[] list = getBuffers();
        for (int x=0;x<list.length;x++) {
            if ( !list[x].hasRemaining() ) {
                continue;
            } else if ( list[x].remaining() < 2 ) {
                list[x].limit(list[x].position());
            }
            list[x].putShort(v);
            break;
        }
    }
    
    @Override
    public void putInt(int v) {
        ByteBuffer[] list = getBuffers();
        for (int x=0;x<list.length;x++) {
            if ( !list[x].hasRemaining() ) {
                continue;
            } else if ( list[x].remaining() < 4 ) {
                list[x].limit(list[x].position());
            }
            list[x].putInt(v);
            break;
        }
    }

    @Override
    public int get(byte[] buf) {
        ByteBuffer[] list = getBuffers();
        int count = 0;
        for (int x=0;x<list.length;x++) {
            if ( !list[x].hasRemaining() ) {
                continue;
            }
            int pos = list[x].position();
            int sw = buf.length-count;
            count += list[x].get(buf,count,(sw > list[x].remaining()) ? list[x].remaining() : sw).position() - pos;
            if ( count == buf.length ) break;
        }
        return count;
    }

    @Override
    public int put(byte[] buf) {
        ByteBuffer[] list = getBuffers();
        int count = 0;
        for (int x=0;x<list.length;x++) {
            if ( !list[x].hasRemaining() ) {
                continue;
            }
            int pos = list[x].position();
            int sw = buf.length-count;
            count += list[x].put(buf,count,(sw > list[x].remaining()) ? list[x].remaining() : sw).position() - pos;
            if ( count == buf.length ) break;
        }
        return count;
    }
    
    @Override
    public void skip(long jump) {
        ByteBuffer[] list = getBuffers();
        long count = 0;
        for (int x=0;x<list.length;x++) {
            if ( !list[x].hasRemaining() ) {
                continue;
            } else if ( jump - count > list[x].remaining() ) {
                count += list[x].remaining();
                list[x].position(list[x].limit());
            } else {
                list[x].position(list[x].position()+(int)(jump-count));
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
    
}
