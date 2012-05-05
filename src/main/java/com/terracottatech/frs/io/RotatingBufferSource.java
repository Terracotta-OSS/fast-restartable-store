/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 *
 * @author mscott
 */
public class RotatingBufferSource implements BufferSource {

    private final ReferenceQueue<ByteBuffer> queue = new ReferenceQueue<ByteBuffer>();
    
    private final TreeMap<Integer,ByteBuffer> freeList = new TreeMap<Integer,ByteBuffer>( new Comparator<Integer>() {
        @Override
        public int compare(Integer t, Integer t1) {
     // make sure nothing ever equals so everything fits in the set
            return t.intValue() - t1.intValue();
        }
    });
    private final HashSet<BaseHolder> used = new HashSet<BaseHolder>();
    private int created = 0;
    private long totalCapacity = 0;
    private static long MAX_CAPACITY = 100L * 1024 * 1024 * 1024;

    @Override
    public ByteBuffer getBuffer(int size) {
        //  super small, just allocate heap
        if ( size < 512 * 1024 ) {
            return ByteBuffer.allocate(size);
        }
        
        clearQueue(totalCapacity > MAX_CAPACITY);
        ByteBuffer factor = checkFree(size);
        int spins = 0;
        while (factor == null) {
            if (totalCapacity > MAX_CAPACITY) {
                freeList.pollLastEntry();
                System.gc();
                clearQueue(true);
                factor = checkFree(size);
            } else {
                // pad some extra for later
                try {
                    int allocate = Math.round(size * 1.05f);
                    if ( allocate < 1024 * 1024 ) allocate = 1024 * 1024 + 8;
                    factor = ByteBuffer.allocateDirect(allocate);
                    created += 1;
                    totalCapacity += factor.capacity();                
                } catch (OutOfMemoryError err) {
                    freeList.pollLastEntry();
                    System.gc();
                    System.out.format("WARNING: ran out of direct memory calling GC for a request of %d.\n",size);
                    clearQueue(true);
                }
            }
            if (spins++ > 100) {
                System.out.format("WARNING: ran out of direct memory for a request of %d.\n",size);
                return null;
            }
        }
        factor = addUsed(factor,size);
        return factor;
    }

    private void clearQueue(boolean wait) {
        try {
            BaseHolder holder = null;
            if (wait) {
                holder = (BaseHolder) queue.remove(1000);
            } else {
                holder = (BaseHolder) queue.poll();
            }
            while (holder != null) {
                if (used.remove(holder)) {
                    if ( freeList.size() < 100 ) {
                        holder.getBase().position(0);
                        ByteBuffer check = holder.getBase();
                        while ( check != null ) {
                            check.limit(check.limit()-1);
                            check = freeList.put(check.limit(),check);
                        }
                    } else {
                        totalCapacity -= holder.getBase().capacity();
                    }
                }
                holder = (BaseHolder) queue.poll();

            };
        } catch (InterruptedException re) {
            throw new RuntimeException(re);
        }
    }

    private ByteBuffer addUsed(ByteBuffer buffer, int size) {
        buffer.clear().position(buffer.capacity()-size);
        ByteBuffer pass = buffer.slice();
        used.add(new BaseHolder(buffer, pass));
        buffer.putInt(buffer.getInt(0)+1,0);
        return pass;
    }

    private ByteBuffer checkFree(int request) {
        if (freeList.isEmpty()) {
            return null;
        }
        Integer get = freeList.ceilingKey(request);
        if ( get == null ) return null;
        
        return freeList.remove(get);
    }

    @Override
    public void reclaim() {
        //  noop
    }

    @Override
    public void returnBuffer(ByteBuffer buffer) {
//  base buffers are always internal so this should just noop

    }

    public int getCount() {
        return created;
    }

    public long getCapacity() {
        return totalCapacity;
    }

    class BaseHolder extends PhantomReference<ByteBuffer> {

        final ByteBuffer baseBuffer;

        public BaseHolder(ByteBuffer base, ByteBuffer t) {
            super(t, queue);
            baseBuffer = base;
        }

        public ByteBuffer getBase() {
            return baseBuffer;
        }
        
        
    }
}
