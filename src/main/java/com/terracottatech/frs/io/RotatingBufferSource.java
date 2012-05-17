/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.util.Formatter;
import java.util.HashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
public class RotatingBufferSource implements BufferSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(IOManager.class);

    private ReferenceQueue<ByteBuffer> queue = new ReferenceQueue<ByteBuffer>();
    
    GlobalBufferSource parent;

    private final HashSet<BaseHolder> used = new HashSet<BaseHolder>();
    private int created = 0;
    private int released = 0;
    private long totalCapacity = 0;
    private long maxCapacity = 100L * 1024 * 1024;
    
    public RotatingBufferSource(long max) {
        maxCapacity = max;
        parent = GlobalBufferSource.getInstance(this,max);
    }

    @Override
    public ByteBuffer getBuffer(int size) {
        
        if ( size < 1024 ) return ByteBuffer.allocate(size);

        clearQueue(totalCapacity > maxCapacity);
        ByteBuffer factor = checkFree(size);
        if ( factor != null && factor.capacity() * .80 > size ) {
            factor = null;
        }
        int spins = 0;
        while (factor == null) {
//            if (totalCapacity > maxCapacity) {
//                parent.reclaim();
//                factor = checkFree(size);
//            } else {
                // pad some extra for later
                try {
                    int allocate = Math.round(size * 1.10f);
                    factor = ByteBuffer.allocateDirect(allocate);
                    created += 1;
                } catch (OutOfMemoryError err) {
                    factor = parent.clear(size);
                    if ( factor != null && factor.capacity() * .80 > size ) {
                        factor = null;
                    }
//                    LOGGER.warn("ran out of direct memory calling GC");
                }
//            }
            if (spins++ > 100) {
                LOGGER.warn("ran out of direct memory");
                return null;
            }
        }
        factor = addUsed(factor,size);
        return factor;
    }
    
    public void clear() {
        clearQueue(false);
        parent.release(this);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        clear();
    }

    private void clearQueue(boolean wait) {
        try {
            BaseHolder holder = null;
            if (wait) {
                while ( holder == null ) {
                    holder = (BaseHolder) queue.remove(1000);
                    if ( holder == null ) System.gc();
                }
            } else {
                holder = (BaseHolder) queue.poll();
            }
            while (holder != null) {
                if (used.remove(holder)) {
                    holder.getBase().position(0);
                    ByteBuffer check = holder.getBase();
                    totalCapacity -= check.capacity();
                    parent.returnBuffer(check);
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
        totalCapacity += buffer.capacity();
        return pass;
    }

    private ByteBuffer checkFree(int request) {
        return parent.getBuffer(request);
    }

    @Override
    public void reclaim() {
        clearQueue(false);
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
