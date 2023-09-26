/*
 * Copyright (c) 2012-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
public class RotatingBufferSource implements BufferSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(IOManager.class);

    private final ReferenceQueue<ByteBuffer> queue = new ReferenceQueue<ByteBuffer>();
    
    private final BufferSource parent;

    private final Set<BaseHolder> used = Collections.synchronizedSet(new HashSet<BaseHolder>());
    private int created = 0;
    private int released = 0;
    private long totalCapacity = 0;
    private int spinsToFail = 0;
    private long millisToWait = 250;
    
    public RotatingBufferSource(BufferSource parent) {
        this.parent = parent;
    }  
    
    public void spinsToFail(int spins) {
        spinsToFail = spins;
    }
    
    public void millisToWait(long millis) {
        millisToWait = millis;
    }

    @Override
    public ByteBuffer getBuffer(int size) {
        ByteBuffer factor = null;
        
        int spins = 1;
        while (factor == null) {
            clearQueue(spins > 1);
            factor = checkFree(size + 8);
            // pad some extra for later

            if (factor == null) {
                if ( !(spinsToFail < 0) && spins++ > spinsToFail ) return null;
                else System.gc();
            }
        }
        factor = addUsed(factor,size);
        return factor;
    }
    
    public void clear() {
        clearQueue(false);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        clear();
    }

    private void clearQueue(boolean wait) {
        try {
            BaseHolder holder;
            if (wait) {
                holder = (BaseHolder) queue.remove(millisToWait);
            } else {
                holder = (BaseHolder) queue.poll();
            }
            while (holder != null) {
                if (used.remove(holder)) {
                    ByteBuffer check = holder.getBase();
                    totalCapacity -= check.capacity();
                    parent.returnBuffer(check);
                }
                holder = (BaseHolder) queue.poll();
            }
        } catch (InterruptedException re) {
            throw new RuntimeException(re);
        }
    }

    private ByteBuffer addUsed(ByteBuffer buffer, int size) {
        if ( !buffer.isDirect() ) {
            return buffer;
        }
        if ( buffer.capacity() < size + 8 ) {
            throw new AssertionError();
        }
        buffer.clear().position(buffer.capacity()-size);
        ByteBuffer pass = buffer.slice();
        used.add(new BaseHolder(buffer, pass));
        buffer.putInt(0,buffer.getInt(0)+1);
        totalCapacity += buffer.capacity();
        return pass;
    }

    private ByteBuffer checkFree(int request) {
        return parent.getBuffer(request);
    }

    @Override
    public void reclaim() {
        clearQueue(false);
    }

    @Override
    public void returnBuffer(ByteBuffer buffer) {
//  base buffers are always internal so this should just noop

    }

    public int getCount() {
        return created;
    }
    
    public int getReleased() {
        return released;
    }

    public long getCapacity() {
        return totalCapacity;
    }

    class BaseHolder extends PhantomReference<ByteBuffer> {

        final ByteBuffer baseBuffer;

        public BaseHolder(ByteBuffer base, ByteBuffer t) {
            super(t, queue);
            baseBuffer = base;
        }

        public ByteBuffer getBase() {
            return baseBuffer;
        }
        
        
    }
}
