/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

/**
 *
 * @author mscott
 */
public class RotatingBufferSource implements BufferSource {

    private final ReferenceQueue<ByteBuffer> queue = new ReferenceQueue<ByteBuffer>();
    
    private final TreeSet<ByteBuffer> freeList = new TreeSet<ByteBuffer>( new Comparator<ByteBuffer>() {
        @Override
        public int compare(ByteBuffer t, ByteBuffer t1) {
     // make sure nothing ever equals so everything fits in the set
            if ( t.capacity() == t1.capacity() ) return -1;
            return t.capacity() - t1.capacity();
        }
    });
    private final HashSet<BaseHolder> used = new HashSet<BaseHolder>();
    private int created = 0;
    private long totalCapacity = 0;
    private static long MAX_CAPACITY = 10L * 1024 * 1024 * 1024;

    @Override
    public ByteBuffer getBuffer(int size) {
        clearQueue(totalCapacity > MAX_CAPACITY);
        ByteBuffer factor = checkFree(size);
        int spins = 0;
        while (factor == null) {
            if (totalCapacity > MAX_CAPACITY) {
                clearQueue(true);
                factor = checkFree(size);
            } else {
                // pad some extra for later
                try {
                    int allocate = Math.round(size * 1.05f);
                    if ( allocate < 1 * 1024 * 1024 ) allocate = 1 * 1024 * 1024 + 8;
                    factor = ByteBuffer.allocateDirect(allocate);
                } catch (OutOfMemoryError err) {
                    System.gc();
                    clearQueue(true);
                }
                created += 1;
                totalCapacity += factor.capacity();
            }
            if (spins++ > 10) {
                return null;
            }
        }
        factor = addUsed(factor,size);
        factor.clear().limit(size);
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
                        freeList.add(holder.getBase());
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
        buffer.position(buffer.capacity()-size);
        ByteBuffer pass = buffer.slice();
        used.add(new BaseHolder(buffer, pass));
        buffer.putInt(buffer.getInt(0)+1,0);
        return pass;
    }

    private ByteBuffer checkFree(int request) {
        if (freeList.isEmpty()) {
            return null;
        }
        Iterator<ByteBuffer> list = freeList.iterator();
        while (list.hasNext()) {
            ByteBuffer c = list.next();
            if (c.capacity() - 8 > request) {
                list.remove();
                return c;
            } else {
                IntBuffer counts = c.asIntBuffer();
                int hits = counts.get(0);
                int misses = counts.get(1);
                if ( misses > 100 ) list.remove();
                else counts.put(misses+1,1);
            }
        }
        return null;
    }

    @Override
    public ByteBuffer[] getBuffers(long size) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void reclaim() {
        //  noop
    }

    @Override
    public void returnBuffer(ByteBuffer buffer) {
//  base buffers are always internal so this should just noop

    }

    @Override
    public void returnByteBuffers(ByteBuffer[] bufs) {
        for (ByteBuffer buf : bufs) {
            returnBuffer(buf);
        }
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
