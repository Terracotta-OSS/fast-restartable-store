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
//        maxCapacity -= max;
    }

    @Override
    public synchronized ByteBuffer getBuffer(int size) {
        if (freeList.isEmpty()) {
            return null;
        }
        Integer get = freeList.ceilingKey(size);
        if ( get == null ) {
            return null;
        }
        ByteBuffer buffer = freeList.remove(get);
        totalSize -= buffer.capacity();
        if ( buffer.capacity() > size * 2 ) {
            ByteBuffer slice = ((ByteBuffer)buffer.clear().position(buffer.capacity()-size)).slice();
            findSlot(((ByteBuffer)buffer.flip()).slice());
            buffer = slice;
        }
        return buffer;
    }

    @Override
    public synchronized void reclaim() {
        Map.Entry<Integer,ByteBuffer> poll = freeList.pollFirstEntry();
        if ( poll != null ) {
            totalSize -= poll.getValue().capacity();
        }
    }

    @Override
    public synchronized void returnBuffer(ByteBuffer buffer) {
        if ( totalSize + buffer.capacity() > maxCapacity ) {
            return;
        }
        findSlot(buffer);
    }
    
    private void findSlot(ByteBuffer buffer) {
        totalSize += buffer.capacity();
        while ( buffer != null ) {
            if ( buffer.limit() == 0 ) {
                totalSize -= buffer.capacity();
                return;
            }
            buffer = freeList.put(buffer.limit(),buffer);
            if ( buffer != null ) buffer.limit(buffer.limit()-1);
        }
    }
    
}
