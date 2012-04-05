/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io;

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * 
 * ChunkIntent 
 *
 * @author mscott
 */
public interface ChunkIntent extends Chunk {
    void transferTo(ByteBuffer[] buf);
//    InputStream getInputStream();
}
