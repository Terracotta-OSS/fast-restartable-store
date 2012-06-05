/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

/**
 *
 * @author mscott
 */
public class FormatException extends Exception {

    private long expected;
    private long calculated;
    private long length;
    
    /**
     * Creates a new instance of
     * <code>ChecksumException</code> without detail message.
     */
    public FormatException(String message, long expected, long calc, long length) {
        super(message);
        this.expected = expected;
        this.calculated = calc;
        this.length = length;
    }

    /**
     * Constructs an instance of
     * <code>ChecksumException</code> with the specified detail message.
     *
     * @param msg the detail message.
     */
    public FormatException(String msg) {
        super(msg);
    }

    @Override
    public String toString() {
        return "FormatException{" + "expected=" + expected + ", calculated=" + calculated + ", length=" + length + '}';
    }
    
    
}
