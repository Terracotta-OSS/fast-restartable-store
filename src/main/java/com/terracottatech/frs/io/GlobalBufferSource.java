/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
import java.util.*;

/**
 *
 * @author mscott
 */
class GlobalBufferSource implements BufferSource {
    
    private static GlobalBufferSource GLOBAL = new GlobalBufferSource();
    
    private long maxCapacity = 0;
    private final CachingBufferSource delegate = new CachingBufferSource();
    private static Map<BufferSource,Long> clients = Collections.synchronizedMap(new HashMap<BufferSource,Long>());
        
    static GlobalBufferSource getInstance(BufferSource client, long capacity) {
        clients.put(client, capacity);
        GLOBAL.addCapacity(capacity);
        return GLOBAL;
    }
    
    static void release(BufferSource client) {
        Long cap = clients.remove(client);
        if ( cap != null ) {
            GLOBAL.releaseCapacity(cap);
            if ( clients.isEmpty() ) GLOBAL = new GlobalBufferSource();
        }
    }
    
    public synchronized void addCapacity(long max) {
        if ( maxCapacity < max ) maxCapacity = max;
    }
    
    public synchronized void releaseCapacity(long max) {
        maxCapacity -= max;
    }

    @Override
    public synchronized ByteBuffer getBuffer(int size) {
        return delegate.getBuffer(size);
    }

    @Override
    public synchronized void reclaim() {
        delegate.reclaim();
    }

    @Override
    public synchronized void returnBuffer(ByteBuffer buffer) {
        if ( delegate.getSize() + buffer.capacity() > maxCapacity ) {
            return;
        }
        delegate.returnBuffer(buffer);
    }
    
}
