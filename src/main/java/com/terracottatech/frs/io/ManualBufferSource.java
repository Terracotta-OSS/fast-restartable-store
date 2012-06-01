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
public class ManualBufferSource implements BufferSource {
    
    private BufferSource parent;
    private long maxCapacity;
    private long usage = 0;
    private int created = 0;
    private int min = Integer.MAX_VALUE;
    private int max = 0;
    private int fails = 0;
    private int failedAllocation = 0;
    private long allocated = 0;
    private ArrayList<BufferWrapper> pool = new ArrayList<BufferWrapper>();

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
        if ( min > size ) min = size;
        if ( max < size ) max = size;
        
        if ( size < 1024 ) return ByteBuffer.allocate(size);
        
        if ( size + usage > maxCapacity ) {
            fails += 1;
            return null;
        }  
            
        ByteBuffer  base = parent.getBuffer(size);
 
        if ( base == null ) {
            base = performAllocation(size);
            if ( base == null ) {
                failedAllocation += 1;
            } else {
                allocated += base.capacity();
                created += 1;
            }
        }

        if ( base != null ) {
           base.clear();
           if ( base.remaining() != size ) {
               base.limit(size);
           }
           BufferWrapper wrap = new BufferWrapper(base);
           assert(!pool.contains(wrap));
           pool.add(wrap);
           usage += base.capacity();
        }
           
        return base;
    }
    
    protected ByteBuffer performAllocation(int size) {
        try {
            int allocate = Math.round(size * 1.05f);
            if ( allocate < 64 * 1024 ) allocate = 64 * 1024;
            ByteBuffer base = ByteBuffer.allocateDirect(allocate);
            return base;
        } catch (OutOfMemoryError err) {
            parent.reclaim();
            return null;
//                    LOGGER.warn("ran out of direct memory calling GC");
        }
    }

    @Override
    public synchronized void reclaim() {
        if ( parent != null ) parent.reclaim();
    }
    
    private long calculateUsage() {
        long val = 0;
        for ( BufferWrapper buffer : pool ) {
            val += buffer.capacity();
        }
        return val;
    }

    @Override
    public synchronized void returnBuffer(ByteBuffer buffer) {
        assert(buffer != null);
        
        if ( pool.remove(new BufferWrapper(buffer)) ) {
            assert(!pool.contains(new BufferWrapper(buffer)));
            usage -= buffer.capacity();
            assert(usage == calculateUsage());
            parent.returnBuffer(buffer);
        } 
    }
   
    public String toString() {
        return "buffer pool created: " + created + " bytes held: " + usage + " capacity: " + maxCapacity +
                " min: " + min + " max: " + max + " overcommit: " + fails + " failedAlloc: " + failedAllocation + " size: " + pool.size();
    }
    
    static class BufferWrapper {
        private final ByteBuffer check;
        
        BufferWrapper(ByteBuffer eq) {
            this.check = eq;
        }
        
        public boolean equals(Object obj) {
            if ( obj instanceof BufferWrapper ) {
                return ((BufferWrapper)obj).check == check;
            } else {
                return super.equals(obj);
            }
        }
        
        public int capacity() {
            return check.capacity();
        }
    }
    
}
