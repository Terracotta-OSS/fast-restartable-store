/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

/**
 * Scan direction for stream read back.
 * @author mscott
 */
public enum Direction {
    FORWARD,REVERSE,RANDOM;
    
    public static Direction getDefault() {
        return REVERSE;
    }
}
