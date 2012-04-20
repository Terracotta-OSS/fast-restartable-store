/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

/**
 *
 * @author mscott
 */
public class RandomGenerator {
    public static byte lessThan(byte x) {
        return (byte) (Math.round(Math.random() * x) & 0xffL);
    }

     public static  short lessThan(short x) {
        return (short) (Math.round(Math.random() * x) & 0xffffL);
    }

     public static  int lessThan(int x) {
        return (int) (Math.round(Math.random() * x) & 0xffffffffL);
    }

     public static  long lessThan(long x) {
        return Math.round(Math.random() * x);
    }
}
