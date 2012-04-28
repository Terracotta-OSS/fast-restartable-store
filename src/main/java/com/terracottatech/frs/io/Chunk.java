/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.nio.ByteBuffer;

/**
 * 
 * Chunk is a collection of ByteBuffers that represents a contiguous 
 * stream of bytes to be transfered to persistent storage.
 *
 * @author mscott
 */
public interface Chunk {
    
    ByteBuffer[] getBuffers();
    
    long length();
    long remaining();
    
    void limit(long v);
        
    boolean hasRemaining();
    
    byte get(long pos);
    short getShort(long pos);
    int getInt(long pos);
    long getLong(long pos);

    byte get();
    void put(byte v);
    byte peek();   
    
    long getLong();
    void putLong(long v);
    long peekLong();
    
    short getShort();
    void putShort(short v);
    short peekShort();

    int getInt();
    void putInt(int v);  
    int peekInt();
    
    int get(byte[] buf);
    int put(byte[] buf);
    
    void skip(long jump);
    ByteBuffer[] getBuffers(long length);
    Chunk getChunk(long length);
    
    void flip();
    void clear();
}
