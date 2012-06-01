/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author mscott
 */
public class Global {
    
    public static ThreadLocal<Future<Void>>   waiters = new ThreadLocal<Future<Void>>();
    public static AtomicLong  count = new AtomicLong();
    
    public static void waitForWrite() {
        Future<Void> w = waiters.get();
        try {
            if ( w != null ) {
                long n = System.nanoTime();
                w.get();
                n = System.nanoTime() - n;
                if ( 1000 % count.incrementAndGet() == 0 ) {
                    System.out.println("wait on write: " + n);
                }
            } 
        } catch ( Exception e ) {
            e.printStackTrace();
        } finally {
            waiters.set(null);
        }
    }
    
    public static void set(Future<Void> w) {
        waiters.set(w);
    }
}
