/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author mscott
 */
public abstract class BufferFilter {
    
    BufferFilter next;
    
    public abstract boolean filter(ByteBuffer buffer) throws IOException;
    public BufferFilter next() {
        return next;
    }
    
    public void add(BufferFilter n) {
        if ( next == null ) next = n;
        else next.add(n);
    }
}
