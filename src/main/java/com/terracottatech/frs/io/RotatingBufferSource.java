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
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 *
 * @author mscott
 */
public class RotatingBufferSource implements BufferSource {
    
    private final ReferenceQueue<ByteBuffer> queue = new ReferenceQueue<ByteBuffer>();
    private final PriorityQueue<ByteBuffer> freeList = new PriorityQueue<ByteBuffer>(10,new Comparator<ByteBuffer>() {

        @Override
        public int compare(ByteBuffer t, ByteBuffer t1) {
            return t.capacity() - t1.capacity();
        }
        
    });
    private final HashSet<BaseHolder> used = new HashSet<BaseHolder>();
    private int created = 0;
    private long totalCapacity = 0;
    private static long MAX_CAPACITY = 10L * 1024 * 1024 * 1024;
    

    @Override
    public ByteBuffer getBuffer(int size) {
        clearQueue();
        ByteBuffer factor = checkFree(size);
        while ( factor == null ) {
            if ( waitForMoreCapacity() ) {
                factor = checkFree(size);
            } else {
                // pad some extra for later
                factor = ByteBuffer.allocateDirect(Math.round(size * 1.05f));
                created+=1;
                totalCapacity += factor.capacity();
            }
        }
        factor = addUsed(factor);
        factor.clear().limit(size);
        return factor;
    }
    
    private void clearQueue() {
        BaseHolder holder = (BaseHolder)queue.poll();
        while ( holder != null ) { 
            if ( used.remove(holder) ) {
                returnBuffer(holder.getBase());
            }
            holder = (BaseHolder)queue.poll();
        };
    }
    
    private synchronized boolean waitForMoreCapacity() {
        boolean waited = false;
        while ( totalCapacity > MAX_CAPACITY ) {
            waited = true;
            try {
                this.wait();
            } catch ( InterruptedException ie ) {
                throw new RuntimeException(ie);
            }
        }
        return waited;
    }
    
    private ByteBuffer addUsed(ByteBuffer buffer) {
        ByteBuffer pass = buffer.duplicate();
        used.add(new BaseHolder(buffer,pass));
        return pass;
    }
    
    private ByteBuffer checkFree(int request) {
        if ( freeList.isEmpty() ) return null;
        Iterator<ByteBuffer> list = freeList.iterator();
        while ( list.hasNext() ) {
            ByteBuffer c = list.next();
            if ( c.capacity() > request ) {
                list.remove();
                return c;
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
        if ( freeList.size() < 100 ) {
            freeList.add(buffer);
        } else {
//  know lifetime creation                created -= 1;
            totalCapacity -= buffer.capacity();
//                drop it on the floor
        }
        this.notifyAll();
    }

    @Override
    public void returnByteBuffers(ByteBuffer[] bufs) {
        for ( ByteBuffer buf : bufs ) {
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
