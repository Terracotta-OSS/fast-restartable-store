/*
 * Copyright (c) 2012-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.TreeMap;

/**
 *
 * @author mscott
 */
public class CachingBufferSource implements BufferSource {
    private long   totalSize;
    private final TreeMap<Integer,ByteBuffer> freeList = new TreeMap<Integer,ByteBuffer>( new Comparator<Integer>() {
        @Override
        public int compare(Integer t, Integer t1) {
     // make sure nothing ever equals so everything fits in the set
            return t - t1;
        }
    });
    
    public CachingBufferSource() {
    }
    
    synchronized int removeSmallest() {
      if ( freeList.isEmpty() ) {
        return 0;
      }
      int size = freeList.remove(freeList.firstKey()).capacity();
      totalSize -= size;
      return size;
    }
    
    synchronized int removeLargest() {
      if ( freeList.isEmpty() ) {
        return 0;
      }
      int size = freeList.remove(freeList.lastKey()).capacity();
      totalSize -= size;
      return size;
    }
    
    @Override
    public synchronized ByteBuffer getBuffer(int size) {
        if (freeList.isEmpty()) {
            return null;
        }
        
        Integer get = freeList.ceilingKey(size);
        if ( get == null ) {
            get = freeList.floorKey(size);
        }
//  don't need to check for null again, already check that the map is not empty
        ByteBuffer buffer = freeList.remove(get);
        totalSize -= buffer.capacity();

        if ( buffer.capacity() < size ) {
            findSlot(buffer);
            return null;
        }
        
        assert(frameIsZeroed(buffer));
        
        buffer.clear();
        buffer.limit(size);
        if ( buffer.capacity() > size * 2 ) {
            buffer.clear();
            buffer.limit(size);
            ByteBuffer slice = buffer.slice();
            buffer.limit(buffer.capacity()).position(size+1);
            findSlot(buffer.slice());
            buffer = slice;
        } else {
            buffer.limit(size);
        }
        assert(buffer == null || buffer.remaining() == size);
        return buffer;
    }
    
    public synchronized long getSize() {
        return totalSize;
    }

    @Override
    public synchronized void reclaim() {
        totalSize = 0;
        freeList.clear();
    }

    @Override
    public synchronized void returnBuffer(ByteBuffer buffer) {
        assert(zeroFrame(buffer));
        findSlot(buffer);
    }
    
    private void findSlot(ByteBuffer buffer) {
        buffer.clear();
        totalSize += buffer.capacity();
        assert(checkReturn(buffer));
        while ( buffer != null ) {
            if ( buffer.limit() == 0 ) {
                totalSize -= buffer.capacity();
                return;
            }
            buffer = freeList.put(buffer.limit(),buffer);
            if ( buffer != null ) buffer.limit(buffer.limit()-1);
        }
    }
    
    private boolean zeroFrame(ByteBuffer buffer) {
        buffer.clear();
        for ( int x=0;x<buffer.capacity();x++) {
            buffer.put(x,(byte)0);
        }
        return true;
    }
    
    private boolean frameIsZeroed(ByteBuffer buffer) {
        if ( buffer == null ) return true;
        buffer.clear();
        for ( int x=0;x<buffer.capacity();x++) {
            if ( buffer.get(x) != 0 ) {
                return false;
            }
        }
        return true;
    }
        
    private boolean checkReturn(ByteBuffer buffer) {
        if ( buffer == null ) return true;
        for ( ByteBuffer b : freeList.values() ) {
            if ( b == buffer ) {
                return false;
            }
        }
        return true;
    }
    
    public int count() {
      return freeList.size();
    }

  @Override
  public String toString() {
    return "CachingBufferSource{" + "totalSize=" + totalSize + ", freeList=" + freeList.size() + '}';
  }
    
    
}
