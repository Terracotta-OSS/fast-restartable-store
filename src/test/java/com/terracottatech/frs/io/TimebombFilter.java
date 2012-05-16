/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author mscott
 */
public class TimebombFilter extends BufferFilter {
    
    private final long start = System.currentTimeMillis();
    private final long explode;
    private boolean exploded = false;
    
    public TimebombFilter(long time, TimeUnit units) {
        explode = start + units.toMillis(time);
    }

    @Override
    public boolean filter(ByteBuffer buffer) throws IOException {
        final Thread hit = Thread.currentThread();
        if ( System.currentTimeMillis() > explode && !exploded) {
            exploded = true;
            new Thread() {
                public void run() {
                    hit.interrupt();
                }
            }.start();
            return true;
        }
        return true;
    }
    
}
