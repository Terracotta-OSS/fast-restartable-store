/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

/**
 *
 * @author mscott
 */
public enum NIOAccessMethod {
    NONE,STREAM,MAPPED;
    
    public static NIOAccessMethod getDefault() {
        return MAPPED;
    }
}
