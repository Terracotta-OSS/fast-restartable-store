/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 *
 * @author mscott
 */
public class SplittingBufferSource implements BufferSource {
  
  private final ByteBuffer base;
  private final int min;
  private final Stack[] cascade;
  private final int HEADER_SIZE = 8;
  

  public SplittingBufferSource(int min, int size) {
    size = 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(size) - 1);
    base = ByteBuffer.allocateDirect(size);
    this.min = min;
    cascade = new Stack[Long.numberOfLeadingZeros(min) - Long.numberOfLeadingZeros(size) + 1];
    for (int x=0;x<cascade.length;x++) {
      cascade[x] = new Stack(x);
    }
    cascade[0].push(base.duplicate());
  }

  @Override
  public ByteBuffer getBuffer(int size) {
    ByteBuffer header = null;
    if ( size > base.capacity() ) {
      header = ByteBuffer.allocateDirect(size);
      header.putInt(4,Integer.MIN_VALUE);
      return header;
    }
    int slot = cascade.length - (Long.numberOfLeadingZeros(min) - Long.numberOfLeadingZeros(size + HEADER_SIZE) + 2);
    int count = 0;
    while ( header == null ) {
      try {
        header = split(slot);
      } catch ( OutOfMemoryError err ) {
        header = consolidate(slot);
        if ( header == null ) {
          this.reclaim();
          if ( count++ == 10 ) {
            return null;
          }
        }
      }
    }  
    header.clear();
    try {
      header.limit(size+HEADER_SIZE).position(HEADER_SIZE);
    } catch ( IllegalArgumentException ia ) {
      return null;
    }
    return header;
  }
  
  private ByteBuffer consolidate(int slot) {
    if ( slot >= cascade.length ) {
      return null;
    }
    ByteBuffer roll = cascade[slot].reclaim();
    if ( roll == null ) {
      roll = cascade[slot].reclaim(consolidate(slot+1));
    }
    if ( roll == null ) {
      roll = cascade[slot].reclaim();
    }
    return roll;
  }
  
  private ByteBuffer split(int slot) throws OutOfMemoryError {
    if ( slot >= cascade.length ) {
      slot = cascade.length - 1;
    } else if ( slot < 0 ) {
        throw new OutOfMemoryError("OOME");
    }
    ByteBuffer split = cascade[slot].pop();
    if ( split == null ) {
      split = split(slot-1);
      int order = split.getInt(4);
      int span = split.capacity()>>1;
      split.clear().limit(span);
      cascade[slot].push(split.slice());
      split.position(split.limit()).limit(split.capacity());
      split = split.slice();
      split.putInt(4, order | (1 << (slot-1)));
    }
    return split;
  }

  @Override
  public void returnBuffer(ByteBuffer buffer) {
    if ( buffer.getInt(4) == Integer.MIN_VALUE ) {
      return;
    }
    int slot = cascade.length  - (Long.numberOfLeadingZeros(min) - Long.numberOfLeadingZeros(buffer.capacity()) + 1);
    cascade[slot--].push(buffer);
  }

  @Override
  public String toString() {
    ArrayList<String> cas = new ArrayList<String>(cascade.length);
    int size = 0;
    for ( Stack s : cascade ) {
      cas.add(s.toString());
      size += s.capacity();
    }
    return "SplittingBufferSource{base=" + base + ", min=" + min + ", cascade=" + cas + ", size=" + base.capacity() + ", total=" + size + "}";
  }

  @Override
  public void reclaim() {
    for ( int x=cascade.length-1;x>0;x-- ) {
      ByteBuffer b = cascade[x].reclaim();
      while ( b!= null ) {
        cascade[x-1].push(b);
        b = cascade[x].reclaim();
      }
    }
  }
  
  int available() {
    int size = 0;
    for ( Stack s : cascade ) {
      size += s.capacity();
    }
    return size;
  }
  
  int largestChunk() {
    for ( Stack s : cascade ) {
      if ( s.pointer > 0 ) {
        return s.blocksz;
      }
    }
    return 0;
  }
  
  private class Stack {
    private int pointer = 0;
    private int max = 0;
    private final ByteBuffer[] slots;
    private final int order;
    private final int blocksz;

    public Stack(int order) {
      this.slots = new ByteBuffer[1 << order];
      this.order = order;
      this.blocksz = base.capacity() / slots.length;
    }
    
    synchronized ByteBuffer pop() {
      if ( pointer == 0 ) {
        return null;
      }
      return slots[--pointer];
    }
    
    synchronized ByteBuffer reclaim(ByteBuffer bb) {
      if ( bb == null ) {
        return null;
      }
      
      if ( pointer > 0 ) {
        ByteBuffer merged = consolidate(bb.getInt(4), slots[pointer-1].getInt(4));
        if ( merged != null ) {
          pointer -= 1;
          return merged;
        }
      }
      
      slots[pointer++] = bb;
      if ( max < pointer ) {
        max = pointer;
      }
      
      return null;
    }
    
    synchronized void push(ByteBuffer bb) {
      if ( bb.capacity() != this.blocksz ) {
        throw new AssertionError("not returning block to proper stack");
      }
      if ( pointer == slots.length ) {
    /* 100% returned, the entire address space is now resident  */
        throw new AssertionError("more returned then allocated");
      }

      slots[pointer++] = bb;
      if ( max < pointer ) {
        max = pointer;
      }
    }
    
    private ByteBuffer consolidate(int ls, int rs) {
      int mask = 1 << (order-1);
      if ( (ls ^ rs) == mask ) {
        int address = ls & ~mask;
        ByteBuffer br = base.duplicate();
        address = address(address);
        br.position(address).limit(address + blocksz*2);
        return br.slice();
      }
      return null;
    }
    
    synchronized ByteBuffer reclaim() {
      if ( pointer < 2) {
        return null;
      }
      
      ByteBuffer twin = slots[pointer-1];
      int check = twin.getInt(4);
      for (int x=0;x<pointer-1;x++) {
        ByteBuffer merged = consolidate(check,slots[x].getInt(4));
        if ( merged != null ) {
          System.arraycopy(slots, x+1, slots, x, pointer-x-2);
          pointer -= 2;
          return merged;
        }
      }
      
      return null;
    }
    
    private int address(int order) {
      int rev = Integer.reverse(order);
      rev >>>= (Integer.SIZE - cascade.length);
      rev *= (base.capacity() / (1 << cascade.length));
      return rev;
    }
    
    synchronized int capacity() {
      int size = 0;
      for ( int x=0;x<pointer;x++ ) {
        size += slots[x].capacity();
      }
      return size;
    }
    
    synchronized boolean isEmpty() {
      return pointer == 0;
    }

    @Override
    public String toString() {
      return "Stack{max=" + max + ", pointer=" + pointer + ", size=" + slots.length + ", holding=" + capacity() + '}';
    }
  }
  
}
