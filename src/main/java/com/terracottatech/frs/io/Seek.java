/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

/**
 *
 * @author mscott
 */
public enum Seek {
    BEGINNING (0),
    END (-1);
    
    Seek(long value) {
        this.value = value;
    }
    
    private long value;
    
    public long getValue() {
        return value;
    }
}
