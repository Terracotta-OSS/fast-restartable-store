/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io;

/**
 *
 * @author mscott
 */
public enum Direction {
    FORWARD,REVERSE;
    
    public static Direction getDefault() {
        return REVERSE;
    }
}
