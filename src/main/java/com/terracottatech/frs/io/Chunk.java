/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
}
