/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 *
 * @author mscott
 */
public class ManualBufferSource implements BufferSource {
    
    BufferSource parent;
    long maxCapacity;
    long usage = 0;
    int created = 0;
    HashMap<Integer,ByteBuffer> pool = new HashMap<Integer,ByteBuffer>();

    public ManualBufferSource(long maxCapacity) {
        this.parent = GlobalBufferSource.getInstance(this, maxCapacity);
        this.maxCapacity = maxCapacity;
    }
    
    
    public ManualBufferSource(BufferSource parent, long maxCapacity) {
        this.parent = parent;
        this.maxCapacity = maxCapacity;
    }

    @Override
    public synchronized ByteBuffer getBuffer(int size) {
        if ( size < 1024 ) return ByteBuffer.allocate(size);
        
        if ( size + usage > maxCapacity ) {
            return null;
        }
        
        ByteBuffer base = parent.getBuffer(size);
        if ( base == null ) {
            try {
                int allocate = Math.round(size * 1.10f);
                base = ByteBuffer.allocateDirect(allocate);
                created += 1;
            } catch (OutOfMemoryError err) {
                parent.reclaim();
//                    LOGGER.warn("ran out of direct memory calling GC");
            }
        } 
        if ( base != null ) {
            ByteBuffer rsrc = ((ByteBuffer)base.clear().position(base.capacity() - size)).slice();
            usage += base.capacity();
            pool.put(System.identityHashCode(rsrc),base);
            return rsrc;
        }
        return null;
    }

    @Override
    public void reclaim() {

    }
    
    private long calculateUsage() {
        long val = 0;
        for ( ByteBuffer buffer : pool.values() ) {
            val += buffer.capacity();
        }
        return val;
    }

    @Override
    public synchronized void returnBuffer(ByteBuffer buffer) {
        assert(buffer != null);
        if ( !buffer.isDirect() ) {
            return;
        }
        
        ByteBuffer base = pool.remove(System.identityHashCode(buffer));
        if ( base != null ) {
            usage -= base.capacity();
            assert(usage == calculateUsage());
            parent.returnBuffer(base);
        } else {
            throw new AssertionError();
        }
    }
   
    public String toString() {
        return "buffer pool created: " + created + " bytes held: " + usage + " capacity: " + maxCapacity;

    }
    
}
