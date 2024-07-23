/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

/**
 * 
 * Chunk is a collection of ByteBuffers that represents a contiguous 
 * stream of bytes to be transfered to persistent storage.
 *
 * @author mscott
 */
public interface Chunk {
    
    ByteBuffer[] getBuffers();
    
    long position();
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
