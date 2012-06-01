/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 *
 * @author mscott
 */
public class MappedChunk extends AbstractChunk {
    
    MappedByteBuffer   loadable;
    ByteBuffer[] view;

    public MappedChunk(MappedByteBuffer l, ByteBuffer slice) {
        loadable = l;
        this.view = new ByteBuffer[] {slice};
    }

    public MappedChunk(MappedByteBuffer l, ByteBuffer[] slice) {
        loadable = l;
        this.view = slice;
    }
    
    public synchronized void load() {
        if ( loadable != null ) loadable.load();
        loadable = null;
    }
    
    public boolean isLoaded() {
        if ( loadable == null ) return true;
        return loadable.isLoaded();
    }
    
    @Override
    public ByteBuffer[] getBuffers() {
        return view;
    }
    
}
