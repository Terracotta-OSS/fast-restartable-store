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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
public class RotatingBufferSource implements BufferSource {
    private final Logger LOGGER = LoggerFactory.getLogger(IOManager.class);

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
    private int released = 0;
    private long totalCapacity = 0;
    private static long MAX_CAPACITY = 100L * 1024 * 1024 * 1024;

    @Override
    public ByteBuffer getBuffer(int size) {
        
        if ( size < 1024 ) return ByteBuffer.allocate(size);

        clearQueue(totalCapacity > MAX_CAPACITY);
        ByteBuffer factor = checkFree(size);
        int spins = 0;
        while (factor == null) {
            if (totalCapacity > MAX_CAPACITY) {
                if ( !freeList.isEmpty() ) {
                    totalCapacity -= freeList.pollLastEntry().getValue().capacity();
                    released += 1;
                    System.gc();
                    clearQueue(true);
                    factor = checkFree(size);
                } else {
                    return null;
                }
            } else {
                // pad some extra for later
                try {
                    int allocate = Math.round(size * 1.05f);
                    if ( allocate < 512 * 1024 ) allocate = 512 * 1024 + 8;
                    factor = ByteBuffer.allocateDirect(allocate);
                    created += 1;
                    totalCapacity += factor.capacity();                
                } catch (OutOfMemoryError err) {
                    if ( !freeList.isEmpty() ) {
                        totalCapacity -= freeList.pollLastEntry().getValue().capacity();
                        released += 1;
                        System.gc();
                        LOGGER.info("WARNING: ran out of direct memory calling GC");
                        clearQueue(true);  
                        factor = checkFree(size);
                    } else {
                        return null;
                    }
                }
            }
            if (spins++ > 100) {
                LOGGER.info("WARNING: ran out of direct memory");
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
                    holder.getBase().position(0);
                    ByteBuffer check = holder.getBase();
                    while ( check != null ) {
                        check.limit(check.limit()-1);
                        check = freeList.put(check.limit(),check);
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
    
    public int getReleased() {
        return released;
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
