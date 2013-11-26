/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * @author mscott
 */
public class SplittingBufferSource implements BufferSource {
  
  private final ByteBuffer base;
  private final Stack[] cascade;
  private final int HEADER_SIZE = 8;
  private final int lowerbound;
  private long timeout = 200L;
  
  public SplittingBufferSource(int min, int max, long timeout) {
    this(min,max);
    this.timeout = timeout;
  }

  public SplittingBufferSource(int min, int size) {

    if ( size > 0x70000000 || size < 0 ) {
      size = 0x70000000;
    }
    
    lowerbound = Integer.numberOfLeadingZeros(min);
    int spread = lowerbound - Integer.numberOfLeadingZeros(size) + 1;
    if ( Integer.numberOfTrailingZeros(size) <  spread ) {
      size |= (0x01 << spread);
      size &= (0xffffffff << spread);
    }
    ByteBuffer create = null;
    while ( create == null ) {
      try {
        create = ByteBuffer.allocateDirect(size);
      } catch ( OutOfMemoryError oome ) {
        if ( spread < 5 ) {
          throw oome;
        } else {
          size = size >> 1;
          spread = lowerbound - Integer.numberOfLeadingZeros(size) + 1;
        }
      }
    }
    base = create;
    cascade = new Stack[spread];
    for (int x=0;x<cascade.length;x++) {
      cascade[x] = new Stack(x);
    }
    cascade[0].push(base.duplicate());
  }

  @Override
  public ByteBuffer getBuffer(int size) {
    ByteBuffer header = null;
    if ( size > base.capacity() ) {
      return null;
    }
    int slot = cascade.length - (lowerbound - Integer.numberOfLeadingZeros(size + HEADER_SIZE) + 2);
    while ( header == null ) {
      try {
        header = split(slot);
      } catch ( OutOfMemoryError err ) {
        header = pauseForMore(slot);
        if ( header == null ) {
          header = consolidate(slot);
          if ( header == null ) {
            header = pauseForMore(slot);
            if ( header == null ) {
              this.reclaim();
              return null;
            }
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
  
  private ByteBuffer pauseForMore(int slot) {
    if ( slot >= cascade.length ) {
      slot = cascade.length - 1;
    }
    if ( timeout > 0 ) {
      return cascade[slot].pauseForMore(timeout);
    }
    return null;
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
      int address = split.getInt(0);
      split.clear().limit(cascade[slot].blocksz);
      cascade[slot].push(split.slice());
      split.position(cascade[slot].blocksz).limit(cascade[slot].blocksz*2);
      split = split.slice();
      split.putInt(4, order | (1 << (slot-1)));
      split.putInt(0, address + cascade[slot].blocksz);
    }
    return split;
  }

  @Override
  public void returnBuffer(ByteBuffer buffer) {
    if ( buffer.getInt(4) == Integer.MIN_VALUE ) {
      return;
    }
    int slot = cascade.length - (lowerbound - Integer.numberOfLeadingZeros(buffer.capacity()) + 1);
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
    return "SplittingBufferSource{base=" + base +  ", cascade=" + cas + ", size=" + base.capacity() + ", total=" + size + "}";
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
    private int waiting = 0;
    private int max = 0;
    private  ByteBuffer[] slots;
    private final int order;
    private final int blocksz;

    public Stack(int order) {
      this.slots = new ByteBuffer[1];
      this.order = order;
      this.blocksz = base.capacity() >> order;
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
      
      add(bb);
      if ( waiting > 0 ) {
        notify();
      }
      return null;
    }
    
    synchronized ByteBuffer pauseForMore(long sleep) {
      if ( pointer == 0 ) {
        try {
          waiting++;
          wait(sleep);
          waiting--;
        } catch ( InterruptedException ie ) {
          Thread.currentThread().interrupt();
          return null;
        }
      }
      if ( pointer > 0 ) {
        return slots[--pointer];
      }
      return null;
    }
    
    private void add(ByteBuffer bb) {
      if ( bb.capacity() != this.blocksz ) {
        throw new AssertionError("not returning block to proper stack " + bb.capacity() + " " + this.blocksz);
      }
      if ( pointer == base.capacity() / this.blocksz ) {
    /* 100% returned, the entire address space is now resident  */
        throw new AssertionError("more returned then allocated");
      } else if ( slots.length == pointer ) {
        slots = Arrays.copyOf(slots, slots.length<<1);
      }
      slots[pointer++] = bb;
      if ( max < pointer ) {
        max = pointer;
      }
    }
    
    synchronized void push(ByteBuffer bb) {
      add(bb);
      if ( waiting > 0 ) {
        notify();
      }
    }
    
  private ByteBuffer consolidate(int ls, int rs) {
      int mask = 1 << (order-1);
      if ( (ls ^ rs) == mask ) {
        int address = ls & ~mask;
        ByteBuffer br = base.duplicate();
        address = address(address);
        br.position(address).limit(address + (blocksz<<1));
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
    
    private int address(int tag) {
      int rev = Integer.reverse(tag);
      rev >>>= (Integer.SIZE - this.order);
      rev *= (blocksz);
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
