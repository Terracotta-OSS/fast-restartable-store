/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 *
 * @author mscott
 */
public class ViewportBufferSource implements BufferSource {
    
    BufferSource parent;
    HashMap<Integer,ByteBuffer> views = new HashMap<Integer,ByteBuffer>();

    public ViewportBufferSource(BufferSource parent) {
    }

    @Override
    public synchronized ByteBuffer getBuffer(int size) {
        ByteBuffer root = parent.getBuffer(size);
        ByteBuffer buffer = root;
        buffer.clear();
        while ( buffer == root ) {
            buffer.limit(size);
            buffer = buffer.slice();
            if ( views.containsKey(System.identityHashCode(buffer)) ) {
                buffer = root;
            }
        }
        views.put(System.identityHashCode(buffer), root);
        return buffer;
    }

    @Override
    public void reclaim() {
        views.clear();
        parent.reclaim();
    }

    @Override
    public synchronized void returnBuffer(ByteBuffer buffer) {
        ByteBuffer root = views.remove(System.identityHashCode(buffer));
        if ( root != null ) {
            parent.returnBuffer(root);
        }
    }
    
}
