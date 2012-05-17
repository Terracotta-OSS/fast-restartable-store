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
    
    BufferSource parent;

    private final HashSet<BaseHolder> used = new HashSet<BaseHolder>();
    private int created = 0;
    private int released = 0;
    private long totalCapacity = 0;
    private boolean noFail = false;
    
    public RotatingBufferSource(BufferSource parent) {
        this.parent = parent;
    }  

    @Override
    public ByteBuffer getBuffer(int size) {
        clearQueue(false);
        ByteBuffer factor = null;
        
        int spins = 1;
        while (factor == null) {
            clearQueue(false);
            factor = checkFree(size + 8);
            // pad some extra for later

            if (spins++ % 10 == 0 ) {
                clearQueue(true);
                factor = checkFree(size + 8);
                if ( factor == null ) {
                    System.gc();
                    LOGGER.info("increase io memory size calling GC");
                    LOGGER.info(parent.toString());
                }
            }
            if ( !noFail && spins > 20 ) return null;
        }
        factor = addUsed(factor,size);
        return factor;
    }
    
    public void setNoFail() {
        noFail = true;
    }
    
    public void clear() {
        clearQueue(false);
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
                holder = (BaseHolder) queue.remove(1000);
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
        if ( buffer.capacity() < size + 8 ) {
            throw new AssertionError();
            
        }
        buffer.clear().position(buffer.capacity()-size);
        ByteBuffer pass = buffer.slice();
        used.add(new BaseHolder(buffer, pass));
        buffer.putInt(0,buffer.getInt(0)+1);
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
