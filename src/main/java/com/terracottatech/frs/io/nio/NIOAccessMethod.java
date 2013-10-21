/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io.nio;

/**
 *
 * @author mscott
 */
public enum NIOAccessMethod {
    NONE,STREAM,MAPPED;
    
    public static NIOAccessMethod getDefault() {
        return STREAM;
    }
}
