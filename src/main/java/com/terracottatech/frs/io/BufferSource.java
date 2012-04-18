/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.nio.ByteBuffer;

/**
 *
 * @author mscott
 */
public interface BufferSource {
    ByteBuffer getBuffer(int size);
    void returnBuffer(ByteBuffer buffer);
    ByteBuffer[] getBuffers(long size);
    void returnByteBuffers(ByteBuffer[] buf);
    void reclaim();
    
}
