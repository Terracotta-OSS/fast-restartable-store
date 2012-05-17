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
    private long   totalSize;
    
    private long maxCapacity = 0;
    
//    private static Set<BufferSource> clients = Collections.synchronizedSet(new HashSet<BufferSource>());
    private static Map<BufferSource,Long> clients = Collections.synchronizedMap(new HashMap<BufferSource,Long>());
    
    private final TreeMap<Integer,ByteBuffer> freeList = new TreeMap<Integer,ByteBuffer>( new Comparator<Integer>() {
        @Override
        public int compare(Integer t, Integer t1) {
     // make sure nothing ever equals so everything fits in the set
            return t.intValue() - t1.intValue();
        }
    });
        
    static GlobalBufferSource getInstance(BufferSource client, long capacity) {
        GLOBAL.reclaim();
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
        maxCapacity += max;
    }
    
    public synchronized void releaseCapacity(long max) {
        maxCapacity -= max;
    }

    @Override
    public synchronized ByteBuffer getBuffer(int size) {
        if (freeList.isEmpty()) {
            return null;
        }
        Integer get = freeList.ceilingKey(size);
        if ( get == null ) return null;
        ByteBuffer buffer = freeList.remove(get);
        totalSize -= buffer.capacity();
        return buffer;
    }

    @Override
    public synchronized void reclaim() {
        for ( BufferSource bs : clients.keySet() ) {
            bs.reclaim();
        }
    }

    @Override
    public synchronized void returnBuffer(ByteBuffer buffer) {
        if ( totalSize + buffer.capacity() > maxCapacity ) {
            return;
        }
        totalSize += buffer.capacity();
        while ( buffer != null ) {
            buffer.limit(buffer.limit()-1);
            buffer = freeList.put(buffer.limit(),buffer);
        }
    }
    
}
