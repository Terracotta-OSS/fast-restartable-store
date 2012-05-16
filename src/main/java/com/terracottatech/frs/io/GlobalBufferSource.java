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
    
    private static Set<BufferSource> clients = Collections.synchronizedSet(new HashSet<BufferSource>());
    
    private final TreeMap<Integer,ByteBuffer> freeList = new TreeMap<Integer,ByteBuffer>( new Comparator<Integer>() {
        @Override
        public int compare(Integer t, Integer t1) {
     // make sure nothing ever equals so everything fits in the set
            return t.intValue() - t1.intValue();
        }
    });
        
    static GlobalBufferSource getInstance(BufferSource client) {
        clients.add(client);
        return GLOBAL;
    }
    
    static void release(BufferSource client) {
        if ( clients.remove(client) && clients.isEmpty() ) {
            GLOBAL = new GlobalBufferSource();
        }
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
        totalSize -= freeList.pollLastEntry().getValue().capacity();
        System.gc();
    }

    @Override
    public synchronized void returnBuffer(ByteBuffer buffer) {
        totalSize += buffer.capacity();
        while ( buffer != null ) {
            buffer.limit(buffer.limit()-1);
            buffer = freeList.put(buffer.limit(),buffer);
        }
    }
    
}
